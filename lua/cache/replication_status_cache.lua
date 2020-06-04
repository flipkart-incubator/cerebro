local constants = require(".lua.common.constants")
local logger = require(".lua.common.logger").new("replication_status_cache.lua")
local util = require(".lua.common.util")
local redis_service = require(".lua.service.redis_connection_manager").get(constants.REDIS_CLUSTER_DPATH)
local lcache = require(".lua.service.local_cache_manager"):new("local_repl_cache")
--local _smctx = require(".lua.sm.state_transistions")

local function log_replication_status(infostring, bktname, status)
	
	local replication_mode  =  status[constants.REPLICATION_STATUS.REPLICATION_MODE]
	local read_cluster      =  status[constants.REPLICATION_STATUS.READ_CLUSTER]
	local write_cluster     =  status[constants.REPLICATION_STATUS.WRITE_CLUSTER]

	logger:info(string.format("%s {Bucket Name : %s}, {READ_CLUSTER : %s}, {WRITE_CLUSTER : %s},{ REPL MODE state : %s}", infostring, bktname, read_cluster, write_cluster, replication_mode))

end

-- Returns the key used in Redis. Adds suffix to avoid collision
-- Param: key - string
-- Returns: string - redis_key as string
local function get_redis_key(key)
	if key == nil then
		return nil
	end
	return key .. constants.REPLICATION_STATUS_REDIS_SUFFIX
end


--~ Sets Key Value Pair for Bucket 

--~ Sets Key Value Pair for Bucket 
local function set(key, value)

	--TBD Should we get the Old state if present and then log the current v/s previos
	--state. For now we are logging the new successfull state
        
	local result = true 
	local repl_mode = value[constants.REPLICATION_STATUS.REPLICATION_MODE]
	local read_cluster = value[constants.REPLICATION_STATUS.READ_CLUSTER]
        local write_cluster = value[constants.REPLICATION_STATUS.WRITE_CLUSTER]
	
	value["version"] = 1.0
	--logger:info(tostring(repl_mode))
	--logger:info(tostring(read_cluster))
	--logger:info(tostring(write_cluster))

	if key == nil or repl_mode == nil or read_cluster == nil or write_cluster == nil then
		logger:error("Invalid Set Request for Replication Status Cache")
		return false
	end
        
	result = redis_service:hmset(get_redis_key(key), value)
	
	result = redis_service:hincrby(get_redis_key(key), "epoch", 1)
        
	if result == true then
		log_replication_status("Replication Status Changed -->" ,key,value)
	end
	
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
	return redis_service:hmget(get_redis_key(key), field)
end

--~ Retrieves values from redis_service. Used by AccountMigrationStatus API to retrieve status value of an Account
local function getall(key)
	return redis_service:hgetall(get_redis_key(key))
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
		logger:debug("Local Entry Does not Exists for Key is -->"..tostring(key))
		local getTuple = getall(key)

		if getTuple ~= nil then
			local localtable = {}
			for idx=1,table.getn(getTuple) do 
				logger:notice("Get Entry is -->"..tostring(getTuple[idx]))
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
			logger:notice("Epoch Same, Reading from Local Cache for Key-->"..tostring(key))
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
					logger:notice("Get Entry is -->"..tostring(getTuple[idx]))
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

return {
	get = get,
	set = set,
	getField = getField,
	log_replication_status = log_replication_status
}
