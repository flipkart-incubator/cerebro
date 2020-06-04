local constants = require(".lua.common.constants")
local logger = require(".lua.common.logger").new("bucket_status_cache.lua")
local util = require(".lua.common.util")
local redis_service = require(".lua.service.redis_connection_manager").get(constants.REDIS_CLUSTER_DPATH)
local lcache = require(".lua.service.local_cache_manager"):new("local_bucket_cache")
--local _smctx = require(".lua.sm.state_transistions")

local function log_bucket_status(infostring, bktname, status)
	
	local active_cluster      =  status[constants.BUCKET_STATUS.ACTIVE]
	local active_sync_cluster =  status[constants.BUCKET_STATUS.ACTIVE_SYNC]
	local bucket_state        =  status[constants.BUCKET_STATUS.STATE]
	local bucket_opmode       =  status[constants.BUCKET_STATUS.OPMODE]

	logger:info(string.format("%s {Bucket Name : %s}, {ACITVE_CLUSTER : %s}, {ACTING_SYNC_CLUSTER : %s},{ bucket_state : %s}, {bucket_opmode : %s}", infostring, bktname, active_cluster, active_sync_cluster, bucket_state, bucket_opmode))

end

-- Returns the key used in Redis. Adds suffix to avoid collision
-- Param: key - string
-- Returns: string - redis_key as string
local function get_redis_key(key)
	if key == nil then
		return nil
	end
	return key .. constants.BUCKET_STATUS_REDIS_SUFFIX
end


--~ Sets Key Value Pair for Bucket 
local function set(key, value)

	--TBD Should we get the Old state if present and then log the current v/s previos
	--state. For now we are logging the new successfull state
 	local result = true 
	local active_cluster = value[constants.BUCKET_STATUS.ACTIVE]
	local active_sync_cluster = value[constants.BUCKET_STATUS.ACTIVE_SYNC]
        local state = value[constants.BUCKET_STATUS.STATE]
        local opmode = value[constants.BUCKET_STATUS.OPMODE]
	value["version"] = 1.0
        
	--Spit out the Table
	logger:info(tostring(active_cluster))
	logger:info(tostring(active_sync_cluster))
	logger:info(tostring(state))
	logger:info(tostring(opmode))
        

	if key == nil or active_cluster == nil or active_sync_cluster == nil or state == nil or opmode == nil then
		logger:error("Invalid Set Request for Bucket Status Cache")
		return false
	end
        
	result = redis_service:hmset(get_redis_key(key), value)

        if result == true then
		logger:notice("BUcket Status Changed TRUE")
		log_bucket_status("Bucket Status Changed -->" ,key,value)
	end
        
	--Update Epoch Here
	result = redis_service:hincrby(get_redis_key(key), "epoch", 1)

	if result == true then
        	logger:info("Updated Bucket Epoch as result --> "..tostring(result))
		log_bucket_status("Epoch Updated Succesfully for  -->" ,key,value)
	end
	--[[
	lbcache:insert_entry(key,value)
	lbcache:print_details()
	lbcache:print_cachetable_details()

	logger:info("Test KV Retrive from Local Cache for Key -->"..tostring(key).." Value is -->" ..tostring(lbcache:get_entry(key,"active")))
	]]--
	--TBD : For Now Bypass the SM Check, Revisit it Later
	--i
	--Check Current Bucket State and the Next Bucket State that it is being migrated to
	--Only update the Cache If a Valid State Transistion Exists
	--Transition from Current State to Self is Considered a Valid State Transisition
	--[[
	local currState = get_bucket_migration_state(key)
	local validTrans = _smctx.isValidStateTransition_Bucket(currState, state)

	if validTrans == false then
		logger:error(string.format("Invalid Bucket State Transistion from : (%s) -> To State (%s)", tostring(currState), tostring(state)))
		result = false
	else
            logger:info(string.format("Trigger Bucket State Transistion from : (%s) -> To State (%s)", tostring(currState), tostring(state)))
	    result = redis_service.hmset(get_redis_key(key), value)

	    if result == true then 
	      	    log_bucket_status("Bucket Status Changed",key,value)
	    end
	end
        ]]--

	return result
end

--~ Retrieves values from redis_service. Used by AccountMigrationStatus API to retrieve status value of an Account
local function get(key, field)
	logger:info("Redis Service Handle is -->"..tostring(redis_service))
	return redis_service:hmget(get_redis_key(key), field)
end

--~ Retrieves values from redis_service. Used by AccountMigrationStatus API to retrieve status value of an Account
local function getall(key)
	logger:info("Redis Service Hande is -->"..tostring(redis_service))
	return redis_service:hgetall(get_redis_key(key))
end

local function delkey(key)
	logger:info("Redis Service Handle is -->"..tostring(redis_service))
	logger:notice("Redis Service Handle DELKEY -->"..tostring(get_redis_key(key)))
	return redis_service:del(get_redis_key(key))
end

----Generic Delete Function	
----First param is the "key", the variable params are the field for which the value 	
----need to be fetched.
local function del(key)
	local local_entry = lcache:exists(get_redis_key(key))
	--If the entry in the local cache is not there read it from remote	
	if local_entry == nil then
		logger:error("Local Entry Does not Exists for Key Whole Deleting -->"..tostring(key))	
	else
	    --Delete tHe Local Cache
	    local res_local = nil	
	    res_local = lcache:delete_entry(get_redis_key(key))
	    logger:notice("Local Cache Delete Result -->"..tostring(res_local).."Key -->"..tostring(get_redis_key(key)))
       end

       --Delete the Remote Cache
       local res = nil
       res = delkey(key)
       logger:notice("Remote Cache Delete Result -->"..tostring(res).."Key -->"..tostring(get_redis_key(key)))	
       return 	
end

--~ Get the Epoch Value of the Current Entry
local function get_kv_epoch(key)
        
	local value = get(key, {constants.CLUSTER_STATUS.EPOCH})

	--~ Table type check is needed because when key is not found in Redis cache, it returns "false", instead of nil. When key is found, Lua table (Array) is only returned.
        if value == nil or type(value) ~= "table" then
                return nil
        end

	if value[1] == ngx.null then
		return nil
	end
        
        return value[1]
end

--Generic Get Get Field Function.
--First param is the "key", the variable params are the field for which the value 
--need to be fetched.
local function getField(key,...)

	local getTable = {}
	local value = {}
	local remote_epoch = get_kv_epoch(key)
        local local_entry = lcache:exists(get_redis_key(key))

	--If the entry in the local cache is not there read it from remote
	if local_entry == nil then
		logger:error("Local Entry Does not Exists for Key is -->"..tostring(key))
		local getTuple = getall(key)

		if getTuple ~= nil then
			local localtable = {}
			for idx=1,table.getn(getTuple) do 
				logger:info("Get Entry is -->"..tostring(getTuple[idx]))
				localtable[getTuple[idx]] = getTuple[idx+1]
				idx = idx + 2
			end
			lcache:insert_entry(get_redis_key(key), localtable)
		
			for k,v in ipairs({...}) do 
				local tuple = lcache:get_entry(get_redis_key(key), v)
				table.insert(value,tuple)
			end
		end
	else 
		local local_epoch = lcache:get_entry(get_redis_key(key),"epoch")
		if local_epoch == remote_epoch then
			logger:info("Epoch Same, Reading from Local Cache for Key-->"..tostring(key))
			for k,v in ipairs({...}) do 
				local tuple = lcache:get_entry(get_redis_key(key), v)
				table.insert(value,tuple)
			end
		else 
			logger:notice("Epoch Updated, Reading from Remote Cache->"..tostring(key))
			local getTuple = getall(key)
                        if getTuple ~= nil then 
				local localtable = {}
				for idx=1,table.getn(getTuple) do 
					logger:info("Get Entry is -->"..tostring(getTuple[idx]))
					localtable[getTuple[idx]] = getTuple[idx+1]
					idx = idx + 2
				end
				lcache:insert_entry(get_redis_key(key), localtable)
		
				for k,v in ipairs({...}) do 
					local tuple = lcache:get_entry(get_redis_key(key), v)
					table.insert(value,tuple)
				end
			end
		end
	end

	return value
end

--~ Sets the Cluster State with Cluster Tuple 

return {
	get = get,
	set = set,
	del = del,
	log_bucket_status = log_bucket_status,
	getField = getField
}
