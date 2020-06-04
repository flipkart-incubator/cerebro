local util = require(".lua.common.util")
local constants = require(".lua.common.constants")
local logger = require(".lua.common.logger").new("user_status_cache.lua")
local redis_service = require(".lua.service.redis_connection_manager").get(constants.REDIS_CLUSTER_DPATH)
local lcache = require(".lua.service.local_cache_manager"):new("local_user_cache")
--local _smctx = require(".lua.sm.state_transistions")

local function log_user_status(infostring, account_name, status)
        local target_write_cluster = status[constants.USER_STATUS.TARGET_WRITE_CLUSTER]
	logger:info(string.format("%s {Account Name : %s},{Target Write Cluster : %s}", infostring, account_name, target_write_cluster))
end


-- Returns the key used in Redis. Adds suffix to avoid collision
-- Param: key - string
-- Returns: string - redis_key as string
local function get_redis_key(key)
	if key == nil then
		return nil
	end
	return key .. constants.USER_STATUS_REDIS_SUFFIX
end

--~ Retrieves values from redis_service. Used by AccountMigrationStatus API to retrieve status value of an Account
--~ Sets the migration status of an account. In turns uses redis_service to write to Redis
local function set(key, value)
	
	local result = true

	if key == nil then 
		return false
	end
        
	if value == nil then
		return false
	end
	
        local write_cluster = value[constants.USER_STATUS.TARGET_WRITE_CLUSTER]
	value["version"] = 1.0

        logger:info("Redis: Trying to Set Account -->"..key .. "Write Cluster State as -> "..write_cluster)
        
        if write_cluster == nil then
		logger:error("Nil Write Cluster Specified for Account-->"..key)
		return false
	end

	result = redis_service:hmset(get_redis_key(key), value)

	if result == true then
		log_user_status("Account Status Changed", key, value)
	else 
		logger:error("Failed to Update User Status Change for -->"..key)
	end

	result = redis_service:hincrby(get_redis_key(key), "epoch", 1)

	--[[TBD Implement Valid State Transiistions Later
	--Check Current Account State and the next Account State that it is being migrated to
	--Only Update the Cache if a Valid State Transistion Exists
	--Transistion from Current State to Self is considered a Valid Transistion
	
        local currState = get_account_migration_state(key)
	local validTrans = _smctx.isValidStateTransition_Account(currState, state)

	if validTrans == false then
		logger:error(string.format("Invalid Account State Transistion from State :  (%s)  -> TO State (%s)", tostring(currState), tostring(state)))
                result = false
	else 
             logger:info(string.format("Trigger Account State Transistion from State :  (%s)  -> TO State (%s)", tostring(currState), tostring(state)))
	     result = redis_service.hmset(get_redis_key(key), value)
	     if result == true then
	   	    log_account_status("Account Status Changed", key, value)         
	     end
        end
        --]]
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
		logger:error("Local Entry Does not Exists for Key is -->"..tostring(key))
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
	set = set,
	get = get,
	getField = getField,
	log_user_status = log_user_status
}
