local constants = require(".lua.common.constants")
local logger = require(".lua.common.logger").new("replicator.lua")
local replication_status_cache = require(".lua.cache.replication_status_cache")
local bucket_status_cache = require(".lua.cache.bucket_status_cache")
local cfgmgr = require(".lua.config_manager.cfgmgr")
local request_builder = require(".lua.request_builder")
local request_dispatcher = require(".lua.request_dispatcher")
local response_dispatcher = require(".lua.response_dispatcher")

-- APi to send a replicated request to a target cluster. This serves both read and write requests.
-- TBD: For read Requests, currently not sure what to do woth the response and it is discarded as
-- of now. 
-- Need to come up with a non blcoking way of sending replication Requests and this entore block
-- will need a revisit
local function send_repl_request(cluster_read, cluster_write, repl_mode, context)

	local cluster_name = nil
        logger:info("Send Replication Request --> Cluster Read"..tostring(cluster_read).."Cluster Write.."..tostring(cluster_write).."Mode .."..tostring(repl_mode))
	
	if repl_mode == constants.REPLICATION_READ then 
		logger:debug("REPLICATION MODE IS READ.."..tostring(repl_mode))
		
		if util.is_write_request(context.request_method) then
                    logger:info("Write Request in Read Only Replication Mode, Bailing Out")
		    return nil
		end
		
		if util.is_delete_request(context.request_method) then
                    logger:info("Delete Request in Read Only Replication Mode, Bailing Out")
		    return nil
		end

		if cluster_read ~= nil then 
			cluster_name = cluster_read
                else 
			logger:error("No READ REplication Cluster Specified")
			return nil
		end
	elseif repl_mode == constants.REPLICATION_WRITE then
		logger:debug("REPLICATION MODE IS WRITE.."..tostring(repl_mode))
		
		if util.is_read_request(context.request_method) then
                    logger:info("Read Request in Write Only Replication Mode, Bailing Out")
		    return nil
		end
		
                if cluster_write ~= nil then 
			cluster_name = cluster_write
                else 
			logger:error("No Write Replication Cluster Specified")
			return nil
		end
	elseif repl_mode == constants.REPLICATION_RW then
		logger:debug("REPLICATION MODE IS READ AND WRITE.."..tostring(repl_mode))
		-- IN case of both Read and Write Requests Repliation, the write cluster is considered
		-- as the target end point for sending the requests
                if cluster_write ~= nil then 
			cluster_name = cluster_write
                else 
			logger:error("No Write Replication Cluster Specified")
			return nil
		end
	end

	logger:info("Target Cluster for Replication is --->"..tostring(cluster_name))

	-- Build request object that can be dispatched by request_dispatcher
	local request = request_builder.build_request(cluster_name, context.access_key, context.request_method)
	local response = request_dispatcher.send_request(request)
	
	local body_size = 0
	if response.body ~= nil then
		body_size = #response.body
	end
        	
	
	logger:notice(string.format("Sent Replicated Request to --> %s,  Request URI --> %s,  Response -->%s, Body Size --> %d", cluster_name, request.uri, response.status, body_size))

        	
        --TBD Print Response Body for Logging Here
	
	return response
end
		
local function replicate_request(context)

	local replStatus = cfgmgr.getreplicationStatus()
	local response = nil

	if replStatus == nil or replStatus == 0 then
		logger:debug("Replication Mode Disabled for this Host")
        else
		--Replicate the Request for the User if Replication is Enabled for the access key
		--The Replication will happen only for a bucket in a unified state, and only when the cluster is not 
		--degraded. (Replication is a BAU activity Only, used fro simulating the loads on test clusters)
		--This is the first layer of redirection where the requests are forwarded to the test cluster directly.
		--A INtelligent Layer can be deployed at the Test Clusters (Using Openresty Itself), to enable the QPS,
		--throttling and controlling the nature of requests that are hitting the test cluster.
		--logger:debug("Sent Request to Target Cluster, Checking for Traffic Replication")
		local replField = replication_status_cache.getField(context.access_key,"repl_mode")
		local repl_mode = replField[1]

		logger:notice("Read Replication Mode as --> "..tostring(repl_mode).." For Bucket .."..tostring(context.bucket_name))
		if repl_mode ~= nil then 
                	if repl_mode ~= constants.REPLICATION_DISABLED then
				
				--Check the Replication Status of this Bucket, before sending the replication Request
				local bucket_repl_mode = nil
				bucket_repl_mode = bucket_status_cache.getField(context.bucket_name, "replication_enabled")
				logger:notice("Bucket Repl Mode is -->"..tostring(bucket_repl_mode[1]))
				if bucket_repl_mode[1] == constants.BUCKET_REPLICATION_ENABLED then
					logger:notice("Sending Traffic for Replication for Bucket -->"..tostring(context.bucket_name))
					local repl_cluster_read = nil
					local repl_cluster_write = nil
					local replField = replication_status_cache.getField(context.access_key,"read_cluster", "write_cluster") 
					repl_cluster_read = replField[1]
					repl_cluster_write = replField[2]
					response = send_repl_request(repl_cluster_read, repl_cluster_write, repl_mode, context)
				end
			end
		end
	end
	return response
end

--API to Handle Anonymous Request

return {
	replicate_request = replicate_request
}

