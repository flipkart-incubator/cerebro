local util = require(".lua.common.util")
local constants = require(".lua.common.constants")
local logger = require(".lua.common.logger").new("cluster_status_cache.lua")
local redis_service = require(".lua.service.redis_connection_manager").get(constants.REDIS_CLUSTER_DPATH)
local lcache = require(".lua.service.local_cache_manager"):new("local_cluster_cache")
--local _smctx = require(".lua.sm.state_transistions")

local function log_cluster_status(infostring, cluster_name, status)
        local epoch = status[constants.CLUSTER_STATUS.EPOCH]
	local version = status[constants.CLUSTER_STATUS.VERSION]
	local cluster_read_epoint= status[constants.CLUSTER_STATUS.CLUSTER_READ_ENDPOINT]
        local cluster_write_epoint = status[constants.CLUSTER_STATUS.CLUSTER_WRITE_ENDPOINT]
        local cluster_state = status[constants.CLUSTER_STATUS.CLUSTER_STATE]
        local fallback_cluster = status[constants.CLUSTER_STATUS.FALLBACK_CLUSTER]
	
	logger:info(string.format("%s --> Epoch %s, Version %s,  Cluster Name %s,  Read Ep %s, , Write EP %s, fallback_cluster %s", 
				infostring, epoch, version , cluster_name, cluster_read_epoint, cluster_write_epoint, cluster_state, fallback_cluster))
end

local function isInvalidClusterState(cluster_state) 
	
	if cluster_state == constants.CLUSTER_STATE_NORMAL then
		return false
	end

	if cluster_state == constants.CLUSTER_STATE_DEGRADED then
	   	return false
	end

	return true
end

-- Returns the key used in Redis. Adds suffix to avoid collision
-- Param: key - string
-- Returns: string - redis_key as string
local function get_redis_key(key)
	if key == nil then
		return nil
	end
	return key .. constants.CLUSTER_STATUS_REDIS_SUFFIX
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

--~ Sets the Cluster State with Cluster Tuple 
local function set(key, value)
	
	local result = true

	if key == nil then 
		return false
	end
        
	if value == nil then
		return false
	end
	
        local cluster_read_epoint= value[constants.CLUSTER_STATUS.READ_ENDPOINT]
        local cluster_write_epoint = value[constants.CLUSTER_STATUS.WRITE_ENDPOINT]
        local cluster_state = value[constants.CLUSTER_STATUS.STATE]
        local fallback_cluster = value[constants.CLUSTER_STATUS.FALLBACK_CLUSTER]
	value["version"] = 1.0

        logger:info("Redis: Set Details for Cluster Name -->"..key)
        logger:info("Redis: Cluster Read EP -->"..cluster_read_epoint)
        logger:info("Redis: Cluster Write EP -->"..cluster_write_epoint)
        logger:info("Redis: Cluster State -->"..cluster_state)
        logger:info("Redis: Fallback Cluster -->"..fallback_cluster)
        
	if cluster_read_epoint == nil then
		logger:error("Nil Read Endpoint for Cluster-->"..key)
                return false
	end
	
	if cluster_write_epoint == nil then
		logger:error("Nil Write ENdpoint for Cluster -->"..key)
                return false
	end

	if cluster_state == nil then
		logger:error("Empty Cluster State for CLuster -->"..key)
		return false
	end
        
	if isInvalidClusterState(cluster_state) then
	   	logger:error("Invalid Cluster State Specified, Rejecting Config")
	   	return false
	end

	if fallback_cluster == nil then
		logger:warn("No Fallback Cluster for specfied Cluster -->"..key)
		
		if cluster_state == constants.CLUSTER_STATE_DEGRADED then 
			logger:error("Fallback Cluster Cannot Be Empty for a Degraded State, Rejecting Config")
			return false
		end
	end

	result = redis_service:hmset(get_redis_key(key), value)

	if result == true then
		log_cluster_status("Cluster Status Updated", key, value)
	else 
		logger:error("Failed to Update Cluster Status Change for -->"..key)
	end

	--Update Epoch Here
	result = redis_service:hincrby(get_redis_key(key), "epoch", 1)

	return result
end

return {
	set = set,
	get = get,
	getField = getField,
	get_kv_epoch = get_kv_epoch,
	log_cluster_status = log_cluster_status,
}
