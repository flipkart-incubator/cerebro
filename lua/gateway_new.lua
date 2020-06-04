local constants = require(".lua.common.constants")
local logger = require(".lua.common.logger").new("gateway_new.lua")
local request_builder = require(".lua.request_builder")
local request_dispatcher = require(".lua.request_dispatcher")
local response_dispatcher = require(".lua.response_dispatcher")
local util = require(".lua.common.util")
local bucket_status_cache = require(".lua.cache.bucket_status_cache")
local user_status_cache = require(".lua.cache.user_status_cache")
local cluster_status_cache = require(".lua.cache.cluster_status_cache")
local statsHandle = require(".lua.stats.statscollector")
local replicator = require(".lua.replicator")
local cfgmgr = require(".lua.config_manager.cfgmgr")
local rate_limiter = require(".lua.rate_limiter.rate_limiter")


local function is_bucket_read_operation(context)
	if context.is_bucket_operation then
		if context.request_method == "GET" then
			return true
		end

		if context.request_method == "HEAD" then
			return true
		end
	end
	return false
end

-- API Related to Bucket Status Check

-- Checks if it's a CREATE_BUCKET request
-- Param: context - Request context
-- Returns: Boolean - If it's a CREATE_BUCKET request
local function is_new_bucket_create(context)
	return context.request_method == "PUT" and context.is_bucket_operation
end

local function is_bucket_delete_op(context)	
	return context.request_method == "DELETE" and context.is_bucket_operation	
end

-- Checks if the request is a DELETE request
-- Param: context - Request context
-- Returns: Boolean - Returns True if the request is a DELETE request and False otherwise
local function is_delete_request(context)
	return context.request_method == "DELETE"
end

-- Checks if the request is a SET_ACL request
-- Param: context - Request context
-- Returns: Boolean - Returns True if the request is a SET_ACL request and False otherwise
local function is_set_acl_request(context)
	return context.request_method == "PUT" and context.is_acl_operation
end

-- Checks if the current request is from Elastic Load Balancer. We are seeing different variations of the healthcheck
-- string. One is plain "/" and the other is "/elb-healthcheck". Currently supporting both these variations.
-- TBD: Need to Come Up with a more Robust way of checking healthchecks from ELB.
-- Param: context - Request context
-- Returns: Boolean - Returns True if the request is from Elastic Load Balancer and False otherwise
local function is_elb_health_check(context)
	--Check if the access key is non empty. There are requests other than Originating from 
	--ELB context, whose URI just contain "/". Example is the get_all_buckets issued from BOTO Context.
	--Under Such Circumstances, checking the access_key helps differentiate bewteen the health checks and 
	--other API calls.
	--
	if context.access_key ~= nil then
		return false
	end

	return context.bucket_name == constants.ELB_REQUEST_LOCATION or context.uri == "/"
end


-- The API expects a request context that needs to be served, as well as the designated primary
-- and secondary clusters for  this request.
-- The requests are first tried in the Primary Cluster and in case of if it is unsuccesfull
	-- Dispatch response if it need not be suppressed
--~ Sends request to required cluster
--~ Param: cluster_name - Cluster where request has to be sent
--~ Param: context - Request context
--~ Param: suppress_dispatch - If response dispatch has to be suppressed
--~ Returns: response obj - Response of the request sent
local function send_request(cluster_name, context, supress_dispatch)

	-- suppress_dispatch is an optional param. By default, it should be false
	if supress_dispatch == nil then
		supress_dispatch = false
	end
        
	-- Build request object that can be dispatched by request_dispatcher
	local request = request_builder.build_request(cluster_name, context.access_key, context.request_method, context.is_v4)
	
	local response = request_dispatcher.send_request(request)
	
	local body_size = 0
	if response.body ~= nil then
		body_size = #response.body
	end
	
	logger:notice(string.format("Sent Request to --> %s,  Request URI --> %s,  Response -->%s, Body Size --> %d", cluster_name, request.uri, response.status, body_size))

	--Store the Response for this request Context from  the Upstream. 
	--Note that in case if the same request is sent to multiple upsstreams, then only the
	--most recent response will be cached.
	context.response_code = response.status
	statsHandle.update_req_stats("endTime")
        statsHandle.print_req_details(context)
        statsHandle.update_local_stats_cache(context)
	
	-- Dispatch response if it need not be suppressed
	if supress_dispatch == false then
		response_dispatcher.dispatch_response(response)
	end
	
	return response
end

-- The API expects a request context that needs to be served, as well as the designated primary
-- and secondary clusters for  this request.
-- The requests are first tried in the Primary Cluster and in case of if it is unsuccesfull
-- the secondary cluster is looked into.
-- It does not make sense to have the primary_cluster and secondary_cluster as same, with respect
-- to the semantics of the API.
local function handle_request_read(context, primary_cluster, secondary_cluster)
	
	logger:notice("Serving Read Request from PC-->"..tostring(primary_cluster) .." SC -->"..tostring(secondary_cluster))
	--Supress the sending of the response when trying to read from the Primary, as this may succed when
	--it is read from the secondary.
	local response1 = send_request(primary_cluster, context, true)
	local response2 = send_request(secondary_cluster, context, true)
	logger:notice("Handling Read Request is PC Resonse-->"..tostring(response1.status).. " SC Response -->"..tostring(response2.status))
        
        -- Honor Dual 200 when both clusters Send a Response , Quite Confusing Ha
        if ((response1.status == 200) and (response2.status == 200)) then
            body_size_1 = #response1.body
            body_size_2 = #response2.body
            
            if (body_size_1 > body_size_2) then
		response_dispatcher.dispatch_response(response1)
	        logger:notice("Dual 200 Read Request Served fronm --->"..tostring(primary_cluster))
            else
	        logger:notice("Dual 200 Read Request Served fronm --->"..tostring(secondary_cluster))
		response_dispatcher.dispatch_response(response2)
            end
        else	
	
            if ((response1.status == 404) or (response1.status == 403)) then
	   	    logger:error("Read Request Failed PC-->"..tostring(primary_cluster).." Trying in the SC -->"..tostring(secondary_cluster))
		    --[[
		    --Read Fallback Count
	            statsHandle.incr_ctr("readfallback_count")
		    ]]--
		    logger:notice("Served Read Request from SC -->"..tostring(secondary_cluster))
		    response_dispatcher.dispatch_response(response2)
	    else
		    logger:notice("Served Read Request from PC -->"..tostring(primary_cluster))
		    response_dispatcher.dispatch_response(response1)
	    end
        end
end

--The API will be called only for SET ACL requests in Buckets which are in failover mode
local function handle_request_write(context, primary_cluster, secondary_cluster)
	
	logger:notice("Serving Write Request from PC-->"..tostring(primary_cluster) .." SC -->"..tostring(secondary_cluster))
	--Supress the sending of the response when trying to read from the Primary, as this may succed when
	--it is read from the secondary.
	local write_response =  send_request(primary_cluster, context, true)
	logger:notice("Handling Write Request is PC Response-->"..tostring(write_response.status))
	if (write_response.status == 404) then
	     write_response =  send_request(secondary_cluster, context, true)
	     logger:notice("Handling Write Request is SC Response-->"..tostring(write_response.status))
	end
        response_dispatcher.dispatch_response(write_response)
end

-- Checks if the current request has to be rejected
-- Currently, we reject DELETE and SET_ACL request from user accounts in MIGRATING state
-- Param: context - Request context
-- Returns: Boolean - True if the request has to be rejected and False otherwise
local function reject_request_in_bucket_split_ctx(context) 
	
	if is_delete_request(context) then
	        statsHandle.incr_ctr("delreq_count")
		logger:info("Recevied Delete Request while Migration In Progress ")
		return 1
	end

	-- TBD : Add Check for CORS Request 
end

local function send_405_response()
	ngx.exit(405)
end

-- Handle the Processing of Requests for Bucket Which Are in Split State
local function handle_req_bucket_split_ctx(context)
            
        
        --Get the ACTIVE and ACTIVE_SYNC Cluster, and the Operation Mode 
	local active_cluster = nil
	local active_sync_cluster = nil
        local bucket_opmode = nil
	local bs = bucket_status_cache.getField(context.bucket_name,"active","active_sync","opmode")
	
	active_cluster      =  bs[1]
	active_sync_cluster =  bs[2]
 	bucket_opmode       =  bs[3]
        
	if active_cluster == nil then 
		logger:error("Bucket Mode Split, No Active Cluster Specified")
		util.send_response(503)
		return
	end
	
	if active_sync_cluster == nil then 
		logger:error("Bucket Mode Split, No Active Sync Cluster Specified")
		util.send_response(503)
		return
	end
	
	if bucket_opmode == nil then 
		logger:error("Bucket Mode Split, No BucketOPMode Specified")
		util.send_response(503)
		return
	end
	
        logger:notice("Bucket State: Split, AC ->"..tostring(active_cluster).." ASC ->"..tostring(active_sync_cluster).." Opmode ->"..tostring(bucket_opmode))
	--The Mock Modes Are For Testing the readiness of the failover cluster, with respect to both read
	--and writes. While passing the live traffic, the requests need to be repliacted as it is both on the
	--AC and ASC cluster. Hence this is placed before the check for rejecting the Requests, which are
	--generally not allowed when the Bucket is in Split State.
        if bucket_opmode == constants.BUCKET_MOCK_FAILOVER then
	       if util.is_read_request(context.request_method) then 
		    --Handle the Read Request
	       	    logger:notice("Bucket --> "..context.bucket_name .. " Mode: MOCK FAILOVER, Request : READ" .." AC -->"..tostring(active_cluster).."ASC -->"..tostring(active_sync_cluster))
		    handle_request_read(context, active_sync_cluster, active_cluster)
		    return
               else 
	       	     logger:notice("Bucket --> "..context.bucket_name .. " Mode: MOCK FAILOVER, Request: WRITE" .." ASC -->"..tostring(active_sync_cluster))
               	     local response1 = send_request(active_sync_cluster, context, true)
		     --This part of the code is being added as a patch to sync to the Active Cluster all the writes that are being recevied
		     --during the bucket failover mode. In a actual bucket degradation, this call may fail. 
	       	     logger:notice("Bucket --> "..context.bucket_name .. " Mode: MOCK FAILOVER, Request: WRITE" .." AC -->"..tostring(active_cluster))
               	     local response2 = send_request(active_cluster, context, true)
		     
		     logger:notice("Response ASC --->"..tostring(response1.status).." Response AC --->"..tostring(response2.status))
		     --Send the Response Back for The Write to ASC Only
		     response_dispatcher.dispatch_response(response1)
		     return
	       end
       end
       
       
        if bucket_opmode == constants.BUCKET_MOCK_RESTORE then 
       		if util.is_read_request(context.request_method) then
	       	    logger:notice("Bucket --> "..context.bucket_name .. " Mode: MOCK RESTORE, Request : READ" .." AC -->"..tostring(active_cluster).."ASC -->"..tostring(active_sync_cluster))
             		--handle_request_read(context, active_sync_cluster, active_cluster)
             		--Reversing the order of the reads, the ASC Cluster is Returning 200 on a prefix match for a Object that does not exist. Until fixed the objects read access patterns
			--are reversed
			handle_request_read(context, active_cluster, active_sync_cluster)
		    	return
	        else
	       	     	logger:notice("Bucket --> "..context.bucket_name .. " Mode: MOCK RESTORE, Request: WRITE" .." AC -->"..tostring(active_cluster))
               		send_request(active_cluster, context)
		    	return
                end
      end
       
      -- This Block of the Code would Get Hit, if we are Running In a Degraded Mode,
       -- basically marking the bucket IN FAILOVER.
       -- In case of Degraded mode, the reads are tried to be served from the ASC and if fails AC
       -- For writes the requests are always sent to the ASC
       if bucket_opmode == constants.BUCKET_FAILOVER then 
	       if util.is_read_request(context.request_method) then 
		    --Handle the Read Request
	       	    logger:notice("Bucket --> "..context.bucket_name .. " Mode: FAILOVER, Request : READ" .." AC -->"..tostring(active_cluster).."ASC -->"..tostring(active_sync_cluster))
		    handle_request_read(context, active_sync_cluster, active_cluster)
		    return
               else 
	       	     logger:notice("Bucket --> "..context.bucket_name .. " Mode: FAILOVER, Request: WRITE" .." ASC -->"..tostring(active_sync_cluster))
		     --if we receive a set request in Failover, and the failover window may be extended for long times
		     --we would need to allow the acl go in a best effort manner
		     --The new writes however would fall to the active_sync_cluster only
		     if is_set_acl_request(context) then 
                         handle_request_write(context, active_sync_cluster, active_cluster)
		     else 
               	        send_request(active_sync_cluster, context)
                     end
		    return
	       end
       end
       
       
	--TBD check for nil for any of the above tuple before proceeding
	-- Block all deletes and set ACL and CORS Modificartioons from user account 
        -- that is being migrated for this particular bucket
	-- TBD: Add Logic for a CORS GET/SET and approproate update in the API belowe
	if reject_request_in_bucket_split_ctx(context) then
		statsHandle.incr_ctr("rejectreq_count")
		logger:error("Request Cannot be Performed Under Bucket Split State")
                send_405_response()
		return
	end		
	
	
	-- For a bucket with OPMODE as MIGRATION_IN_PROGRESS the following is the flow
	-- The reads for the bucket are tried in the Active Cluster First (From Where the Bucket is being Migrated)
	-- If this fails the reads are tried in the the Active Sync Cluster (To Where the Bucketr is benng Migrated)
	-- In case of writes, for a bucket in migration the requests are always send to the Active Sync Cluster
        if bucket_opmode == constants.BUCKET_MIGRATION_IN_PROGRESS then
		if util.is_read_request(context.request_method) then
		    --Handle the Read Request
	       	    logger:notice("Bucket --> "..context.bucket_name .. " Mode: MIGRATION, Request : READ" .." AC -->"..tostring(active_cluster).."ASC -->"..tostring(active_sync_cluster))
		    handle_request_read(context, active_cluster, active_sync_cluster)
		    return
	        else
		    --Handle the Write Request
	       	    logger:notice("Bucket --> "..context.bucket_name .. " Mode: MIGRATION, Request: WRITE" .." ASC -->"..tostring(active_sync_cluster))
		    send_request(context,active_sync_cluster)
		    return
                end
       end
       
       -- This Block of the Code would Get Hit, if we are Recovering from a  Degraded Mode,
       -- Marking the Bucket in RESTORE
       -- In case of Restore ode, the reads are tried to be served from the ASC and if fails AC
       -- For writes the requests are always sent to the AC (This is the difference between FAILOVER and RESTORE Mode)

       if bucket_opmode == constants.BUCKET_RESTORE then 
       		if util.is_read_request(context.request_method) then
	       	    logger:notice("Bucket --> "..context.bucket_name .. "Mode: RESTORE, Request: READ" .." AC -->"..tostring(active_cluster).."ASC -->"..tostring(active_sync_cluster))
             		handle_request_read(context, active_sync_cluster, active_cluster)
		    	return
	        else
	       	     	logger:notice("Bucket --> "..context.bucket_name .. "Mode: RESTORE, Request: WRITE" .." AC -->"..tostring(active_cluster))
               		send_request(active_cluster, context)
		    	return
                end
      end
end

--API for Handling Bucket Operations, when the Bucket is in Unified State.
--The Majority of the Data Flow would always be through the Unified State of the Bucket
local function handle_req_bucket_unified(context)
	local target_cluster = nil
	local fallback_cluster = nil
	local cluster_state = nil
	local orig_response = nil
	local replication_response = nil

	local bs = bucket_status_cache.getField(context.bucket_name,"active")
	
	if bs[1] ~= nil then 
		target_cluster = bs[1]
	end
	
	local clusterField = cluster_status_cache.getField(target_cluster,"cluster_state")
	cluster_state = clusterField[1]
	    
	logger:info("Bucket State:UNIFIED Bucket Name-->"..tostring(context.bucket_name).."Cluster State -->"..tostring(cluster_state))
	
	if target_cluster == nil then
		logger:error("FATAL, Unknow Target Cluster, BOOTSTRAPPING to DEFAULT CLUSTER (BUG)")
		target_cluster = context..bootstrap_cluster
	end
	orig_response = send_request(target_cluster, context, true)
	--End Of Flow of Handling the Request
		

	if util.is_success_code(orig_response.status) then
		--Replicate the Request if Necessary, only if original response is Valid
		replication_response = replicator.replicate_request(context)
	end

	if replication_response == nil then 
		--Send the Original Response Back
		response_dispatcher.dispatch_response(orig_response)
		return		
	end
	
	if util.is_success_code(replication_response.status) then
		response_dispatcher.dispatch_response(orig_response)
	        return
	else
		statsHandle.incr_ctr("repl_failed_response")
		response_dispatcher.dispatch_response(orig_response)
		--response_dispatcher.dispatch_response(replication_response)

	end
	return
end

--API to Handle Anonymous Request
local function handle_anonymous_request(context)
        
	if util.is_write_request(context.request_method) then
		statsHandle.incr_ctr("anon_write")
	else 
		statsHandle.incr_ctr("anon_read")
	end


	--Send Anonymous Request to Active Sync Clutser. 
	--If this fails send to the Active Cluster.
	local bucket_status = {}
	bucket_status = bucket_status_cache.getField(context.bucket_name,"active","active_sync","access_key")

        local active_cluster = bucket_status[1]
        local active_sync_cluster = bucket_status[2]
	local access_key = bucket_status[3]
	
	--BOOTSTRAP CODE START
	if active_sync_cluster == nil then
		active_sync_cluster = context.bootstrap_cluster
	end
	if active_cluster == nil then
		active_cluster = context.bootstrap_cluster
	end
	--BOOTSTRAP CODE END
	        
	logger:info("Anonymous Request, Active Sync Cluster--->"..tostring(active_sync_cluster))
	local response_asc = send_request(active_sync_cluster, context, true)
	if util.is_success_code(response_asc.status) then 
		response_dispatcher.dispatch_response(response_asc)
	else
		logger:info("Anonymous Request, Active Cluster--->"..tostring(active_cluster))
		local response_ac = send_request(active_cluster, context, true)
		response_dispatcher.dispatch_response(response_ac)
	end
	return
end


--API to Handle The Bucket Create Request. 
--The Bucket Create Request Can Come in Either of the states, when the cluster/user is marked degraded.
--When the Cluster is marked degraded, the target cluster is found using the Fallback Cluster
--In case of the User, the target cluster can also be found by using the the target write cluster.
local function handle_bucket_create_request(context)
	local target_cluster = nil
	local fallback_cluster = nil
	local target_bucket_write_cluster = nil
	local cluster_state = nil
	local placement_policy = nil

	logger:info("Bucket Create Request Handling") 
	--This is a new Bucket Create, and at this point there bo no entry in the bucket_status_cache for the same.
	--We need to Query the Corresponding User/Access_key as to which is the current cluster where new writes are
	--happening.
	local userField = user_status_cache.getField(context.access_key,"target_write_cluster","placement_policy")
	if userField ~= nil then 
		target_cluster = userField[1]
		placement_policy = userField[2]
		logger:notice("Bucket Create Target Cluster-->"..tostring(target_cluster).." Placement Policty -->"..tostring(placement_policy))
	end
	
	--BOOTSTRAP CODE START
	if target_cluster == nil then
		target_cluster = context.bootstrap_cluster
		logger:notice("BCreate BOOT STRAPPING->"..tostring(target_cluster))
	end
	--BOOTSTRAP CODE END
	
	target_bucket_write_cluster = target_cluster
	    

	logger:notice("Bucket Create: -->"..context.bucket_name.."-->Cluster for Writing the bucket is -->"..target_cluster)
	--TBD Frame the Query URI with the placement Policy before sending the send request to target
        

	--Check if this bucket already exists. The Best way to do so would be by actually sending a 
	--a call to the EndPoint. Currently We are replying on the entry in the Cache, to validate.
	--Possibilty of a remote race condition
	
	local is_bkt_already_created = nil
	is_bkt_already_created = bucket_status_cache.getField(context.bucket_name,"access_key")
	logger:notice("The Value is -- >"..tostring(is_bkt_already_created[1]))
	if is_bkt_already_created[1] ~= nil then
		if context.access_key ~= nil then 
		    if is_bkt_already_created[1] == context.access_key then 
		           util.send_response(409, "BucketAlreadyOwnedByYou")
	            end
		end
		util.send_response(409, "BucketAlreadyExists")
		--Frame A Informatinal Response, in the mean time send a 403
		--local response = nil
		--response.status = 204
		--response.body = "Bucket Already Exists"
		--response_dispatcher.dispatch_response(response)
		return
	end
	
	local response = send_request(target_bucket_write_cluster, context, true)
	logger:notice("Bucket Create Response is -->"..tostring(response.status)) 
	    
	-- Update cache only if the cluster operation succeeds
	-- Known Issue: If cluster op succeeds and cache update op fails, 
	-- we are returning a failure response to the user hoping retries would go through. 
	-- Basically, we would be a momentary inconsistency in actual cluster's state and state managed by redis.
	--The Bucket Can be Written in two Modes . If the Bucket is being written when the cluster state is normal, then the bucket 
	--AC and ASC are the same (Which is the Entry from the User Status Cache, as the Target Write Cluster)
	
	 if util.is_success_code(response.status) then
         	local bucket_detail = {}
	        bucket_detail = { active = target_cluster, 
		                  active_sync = target_cluster, 
				  state = constants.BUCKET_STATE_UNIFIED, 
			          opmode = constants.BUCKET_OP_INVALID,
				  access_key = context.access_key,
				  replication_enabled = constants.BUCKET_REPLICATION_ENABLED
				}
	        
		local cache_set_status = bucket_status_cache.set(context.bucket_name, bucket_detail)
		-- TBD: Check if set is a sync call, or add a retry mechnism
		-- Return success response only if cache update succeeds
		if cache_set_status == true then
		    logger:info("Bucket Create : Success, Sending Response for -->"..context.bucket_name)
		    --Update the Bucket Create Stats
		    statsHandle.update_bkt_create_stats(context)
		    --Replicate this Request if Needed
	            
		    local repl_response = replicator.replicate_request(context)
		    if repl_response == nil or util.is_success_code(repl_response.status) then
		    	response_dispatcher.dispatch_response(response)
		    else 
			statsHandle.incr_ctr("repl_failed_response")
			response_dispatcher.dispatch_response(response)
			--response_dispatcher.dispatch_response(repl_response)
		    end

		    return
		else
		    logger:error("Bucket Create : Error in Updating Cache --> "..context.bucket_name)
		    util.send_response(500)
		    return
		end
	    else
		logger:error("Bucket Create : Error While Trying to Create a new Bucket --> " ..context.bucket_name.."Code --> "..tostring(response.status))
		response_dispatcher.dispatch_response(response)
		return
	 end
	 return
end

---API to Handle The Bucket Delete Request.
local function handle_bucket_delete_request(context)
	local bkt_field = {}
	local user_field = {}
	local access_key = nil
	local active_cluster = nil

	user_field = user_status_cache.getField(context.access_key, "target_write_cluster")	
	bkt_field = bucket_status_cache.getField(context.bucket_name,"access_key")	
        
	if user_field[1] ~= nil then	
            active_cluster = user_field[1]	
	end	

        if bkt_field[1] ~= nil then	
	     access_key = bkt_field[1]	
        end

	----Before Deleting Validate, that the access_key (User) that is requesting the delete of the bucket	
	----is actually the owner of the same. 	
	----Handling of Rogue Clients (Especailly badly coded cron, or abandoned jobs) trigger deletes on very old	
	----buckets that even do not exist	
	
	if context.access_key ~= access_key then	
	    logger:error("Error, Bucket in Delete -->"..context.bucket_name.. "Does not belong to user -->"..context.access_key)	
	    local response = send_request(active_cluster, context, true)	
	    response_dispatcher.dispatch_response(response)	
	    util.send_response(403, "Bucket "..context.bucket_name.." NOTOWNEDBYYOU")	
	    return	
	end	
	
	logger:notice("Deleting Bucket Name -->"..context.bucket_name)

	--Send the Request to the Endpoint	
	local response = send_request(active_cluster, context, true)	
	
	if util.is_success_code(response.status) then	
	    --Send the Request to the Cache	
	    logger:notice("Deleting Bucket Local Cache-->"..context.bucket_name)	
	    bucket_status_cache.del(context.bucket_name)		
	else 	
	    logger:error("Failed to Delete Bucket")	
	end	
	
        response_dispatcher.dispatch_response(response)	
        return	
end

-- Handles ALL requests
-- Param: context - Request context
-- Returns: nil
local function handle_request(context)
        
	local key = context.access_key
	
	if context.bucket_name == nil and key == nil then
		logger:error("Malformed Request Empty Bucket and Nil Key")
		util.send_response(400)
		return
	end
	
	--This is a Anynonymous Request. This can come for Any of the buckets (in 
	-- either case of UNIFIED and SPLIT, The first call should be to the active cluster
	-- If that fails it should be the active sync cluster
	if key == nil then
		handle_anonymous_request(context)
		return
	end

	--This signature matches the HEAD call, where the bucket name is nil and the key is valid
	--In this case the Cluster information needs to come from the User Status Cache (TBD)
	--(The Fucntionality is being placed for the repos bucket sync to samit)
	--TBD :- If the USer has its bucket spread across more than a cluster, than the response 
	--needs to be concated before sending back.
	--Under Migration or Degradation, we can return a partial result.For a BAU this needs to be concatanated.
	if context.bucket_name == nil and key ~= nil then
		logger:notice("Reading from Here User Status Cache as-->"..context.access_key)
		--User Status Cache Will be Warmed Up by a external Client
		local userField = user_status_cache.getField(context.access_key,"target_write_cluster")
		local target_cluster = userField[1]

		--BOOTSTRAP CODE START
		if target_cluster == nil then
			target_cluster = context.bootstrap_cluster
		end
		--BOOTSTRAP CODE END
		
		local response = send_request(target_cluster, context)
	        return
	end
        
	--Check if this is a READ Bucket Request as in, it is a HEAD or a GET CALL Made for the existence of
	--the bucket. 
	if  is_bucket_read_operation(context) then
		logger:notice("Bucket Request Type (GET / HEAD) Recevied for Bucket -->"..context.bucket_name .."Akey -->"..context.access_key)
		local userField = user_status_cache.getField(context.access_key,
						  	"target_write_cluster")

		local target_write_cluster = userField[1]
		local response = send_request(target_write_cluster, context)
		logger:notice("Response Sent to Client  -->"..tostring(response.status))
		return	
	end
	
	-- A new Bucket Create, would be defined by the USER/ACCESS_KEY as to where the requests would be routed to.
	-- If it's a create bucket request, route to target cluster of the user account
	local bkt_create = is_new_bucket_create(context)
	if bkt_create == true then
		handle_bucket_create_request(context)
		return
	end

	local bkt_delete = is_bucket_delete_op(context)
	if bkt_delete == true then
		handle_bucket_delete_request(context)
		return
	end


        --Pass the Bucket Name Here, and not the User Name the Bucket Cache Understands the Bucket Keys
	local bucketField = bucket_status_cache.getField(context.bucket_name,"state")
	local bucket_state = nil

	if bucketField ~= nil then
		bucket_state = bucketField[1]
		logger:notice("Bucket State Read is -->"..tostring(bucket_state))
	end
       	
	--Some clientts like the s3cmd and custom written clinets, try to get information about the bucket, before even
	--creating them. Eg Sending the location=? Prefix. For these requests, which are not created, they are directly sent
	--to the endpoint, and the response is passed back. No entry is made into the DB.
	--Initially there was a Bootstraping Code, that used to insert the entries if they are not present, but it has been deprecated
	--from hereon.
	if bucket_state == nil then
		logger:notice("Some Special Request for Bucket : --->"..context.bucket_name.."For Akey -->"..context.access_key)
		local userField = user_status_cache.getField(context.access_key,
						  	"target_write_cluster")

		local target_write_cluster = userField[1]
		local response = send_request(target_write_cluster, context)
		logger:notice("Response Sent to Client  -->"..tostring(response.status))
		return
		--[[
		local userField = user_status_cache.getField(context.access_key,
						  	"target_write_cluster")
		local target_cluster = userField[1]
		local bucket_detail = { active = target_cluster, 
		                        active_sync = target_cluster, 
					state = constants.BUCKET_STATE_UNIFIED, 
					opmode = constants.BUCKET_OP_INVALID,
				        access_key = context.access_key,
				  	replication_enabled = constants.BUCKET_REPLICATION_ENABLED
		
		}
		local cache_set_status = bucket_status_cache.set(context.bucket_name, bucket_detail)
		logger:notice("BootStrap Bucket Success : --->"..context.bucket_name)
		if cache_set_status == true then
		    logger:info("BootStrap Bucket : Success")
		else
		    util.send_response(500)
		    return
		end
       		--Update the Bucket_State Variable after Boot Straping
		bucket_state = (bucket_status_cache.getField(context.bucket_name,"state"))[1]
		util.send_response(500)
		]]--
	end
        --BootStrap Code End
	
	--Get the Bucket State. If it is in Unified State then We Route to the Active Cluster.
	--If the state of the Bucket Is Split, it would be due to either of the following reasons:-
	--  
	--  1. There is a bucket Migration Going on, in Which case, the order of the Transactions for a 
	--     request for a bucket would be ACTIVE_SYNC, followed by ACTIVE Cluster. 
	--     The Bucket Operation Mode Would be MIGRATION_IN_PROGRESS, followed by MIGRATION_COMPLETED
	--  
	--  2. In case of a temporary failure, of the ACTIVE Cluster, all the Read/Writes would be sent to
	--  the ACTIVE_SYNC Cluster.
	--     During the BUCKET_FAILOVER mode All the Transaction would be sent to only the ACTIVE_SYNC Cluster.
	--     During the BUCKET_RESTORE mode the read transactions would be sent as:-
	--             ACTIVE_SYNC -> ACTIVE
	--     During the BUCKET_RESTORE mode the write transaction would be sent to:-
	--            ACTIVE cluster
	--  
	--  If the Bucket is in BUCKET_STATE_SPLIT, DELETE and CORS Operations would be Blocked*
        
        
	--Check if this is a READ Bucket Request as in, it is a HEAD or a GET CALL Made for the existence of
	--the bucket. 
	
	--If the bucket is in UNIFIED state we always refer to the the Active Cluster for the State
	if bucket_state == constants.BUCKET_STATE_UNIFIED then
	    	logger:notice("Bucket State : UNIFIED Bucket Name-->"..context.bucket_name)
		handle_req_bucket_unified(context)
		return
        end

	if bucket_state == constants.BUCKET_STATE_SPLIT then 
	    logger:info("Bucket State : SPLIT Bucket Name-->"..context.bucket_name)
	    handle_req_bucket_split_ctx(context)
	    return
        end
        
	-- One of  the Possible Scenarios of reaching here is that eother the Bucket States is read wrong or nil
	
	logger:error("FATAL Unreachable Code, Should Not Happen.. Bucket State is -->"..tostring(bucket_state))
	util.send_response(503)
end

-- Routes request to required cluster. Entry point of all requests
-- Param: context - Request context
-- Returns: nil
local function route_request(context)
        
	statsHandle.update_req_stats("reqId")
	statsHandle.update_req_stats("startTime")
	
	local rate_limit_breached = false
        local return_code = nil
	local message = nil

	-- If the request is from ELB, return success response
	if is_elb_health_check(context) then
		logger:debug("Sent ELB Health Check Success Response")
	    	util.send_response(200)
	    	return
	end
       
	--Check Rate Limit for the Request before proceeding further	
        rate_limit_breached, return_code, message = rate_limit_breachedrate_limiter.check_rate_limit(context)
	if rate_limit_breached == true then
		util.send_resposne(return_code, message)
		return
	end


	-- repo_svc user special handling - return 429 if it's from an invalid repo_svc user
	if is_proxy_repo_svc_user(context) == false then
		util.send_response(429)
		return
	end 
	
	handle_request(context)
end

-- Handles ERROR scenarios
-- This is executed when some unhandled exception is raised. It routes to default cluster
-- Param: error_message - Error message
-- Returns: nil
local function fallback(error_message, context)
	logger:error("Error While Serving Request --> " ..error_message .."Sending Request to Default Cluster")
	statsHandle.incr_ctr("fallback_count")
	send_request(context.bootstrap_cluster, context)
end

return {
	route_request = route_request,
	fallback = fallback
}
