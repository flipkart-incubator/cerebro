local constants = require(".lua.common.constants")
local logger = require(".lua.common.logger").new("redis_service.lua")
local redis = require(".lua.redis.redis")

local redis_service_handle = {}
local redis_service={}
redis_service.__index = redis_service

function redis_service_handle:new(clusterName)
    
    if clusterName == nil then
	 logger:error("Nil Cluster Name Specified for Service Creation")
	 return nil
    end
    logger:notice("Initializing Redis Connection for Cluster --> "..tostring(clusterName)) 
    local self = {    
                logger:notice("ROMISRA calling Connection Factory--> "..tostring(clusterName)),
    		cacheHandle = require(".lua.service.redis_connection_factory"):new(clusterName),
    }
    
    if self.cacheHandle == nil then 
	    logger:error("Could not Get Connection from Connection Factory for ClusterName"..tostring(clusterName))
	    return nil
    end
    logger:notice("Init Cache, Cache Handle is -->"..tostring(self.cacheHandle))
    setmetatable(self,redis_service)
    return self
end

-- Puts Redis Objects into Connection Pool, to be reused later
-- Using Redis Object after it is put into connection Pool, for operation
-- other than connect, will return a error (Closed State)
-- Param: red - Redis object
-- Returns: nil
local function set_keep_alive(red)

	local res, err = red:set_keepalive(constants.REDIS_KEEPALIVE_TIMEOUT, 
	                                            constants.REDIS_POOL_SIZE)
	if res == nil then
        	logger:error(string.format("Setting Redis Keepalive Status Code OK : %s err : %s ", 
					tostring(ok), tostring(err)))
	else 
        	logger:debug(string.format("Setting Redis Keepalive Status Code OK : %s err : %s ", 
	                            tostring(ok), tostring(err)))
	end

end

-- Gets value of given key
-- Param: key - Key for which value has to be retrieved
-- Returns: value
function redis_service:hincrby(key, field, value)
        local cacheHandle = nil
	cacheHandle = self.cacheHandle

	local red = cacheHandle:get_redis_connection(constants.REQUEST_TYPE_WRITE)

	if red == nil then
		logger:error("Error While Getting Redis Connection for Request -> READ")
		return false
	end

	local res, err = red:hincrby(key, field, value)

	logger:info(string.format("Redis Service HINCRBY %s %s", tostring(res), tostring(err)))

	if res == nil then
		logger:error("Error while redis_service.HINCRBY for Write" .. tostring(err))
		return false
	end

	--Put Redis Object Back Into Connection Pool
	set_keep_alive(red)

	return true
end

-- Gets values of given fields in the given key's value
-- Param: key - Key

-- Gets value of given key
-- Param: key - Key for which value has to be retrieved
-- Returns: value
function redis_service:get(key)
        local cacheHandle = nil
	cacheHandle = self.cacheHandle

	local red = cacheHandle:get_redis_connection(constants.REQUEST_TYPE_READ)

	if red == nil then
		logger:error("Error While Getting Redis Connection for Request -> READ")
		return nil
	end

	local res, err = red:get(key)

	logger:info(string.format("Redis Service get %s %s", tostring(res), tostring(err)))

	if res == nil then
		logger:error("Error while redis_service.get for Read" .. tostring(err))
	end

	--Put Redis Object Back Into Connection Pool
	set_keep_alive(red)

	return res
end

function redis_service:hgetall(key)
        local cacheHandle = nil
	cacheHandle = self.cacheHandle

	local red = cacheHandle:get_redis_connection(constants.REQUEST_TYPE_READ)

	if red == nil then
		logger:error("Error While Getting Redis Connection for Request -> READ")
		return nil
	end

	local res, err = red:hgetall(key)

	logger:debug(string.format("Redis Service getall %s %s", tostring(res), tostring(err)))

	if res == nil then
		logger:error("Error while redis_service.get for Read" .. tostring(err))
	end
	
	if table.getn(res) == 0 then
		logger:debug("HGETALL Non Existing Key in Redis -->"..tostring(key))
		set_keep_alive(red)
		return nil
	end
	

	--Put Redis Object Back Into Connection Pool
	set_keep_alive(red)

	return res
end

function redis_service:del(key)
        local cacheHandle = nil
	local red = nil
	
	cacheHandle = self.cacheHandle
	red = cacheHandle:get_redis_connection(constants.REQUEST_TYPE_WRITE)

	if red == nil then
		logger:error("Error While Getting Redis Connection for Request -> WRITE")
		return nil
	end
        --Del Returns a Number, not a table
	local res, err = red:del(key)

	logger:debug(string.format("Redis Service DEL %s %s", tostring(res), tostring(err)))
	if res == nil then
		logger:error("Error while redis_service DEL " .. tostring(err))
	end
	
	--Put Redis Object Back Into Connection Pool
	set_keep_alive(red)

	return res
end

-- Gets values of given fields in the given key's value
-- Param: key - Key

-- Gets values of given fields in the given key's value
-- Param: key - Key
-- Param: field - List of fields for which values have to be retrieved
-- Returns: Lua Table - Returns values of given fields
function redis_service:hmget(key, field)

        local cacheHandle = nil
	cacheHandle = self.cacheHandle
	--[[
	logger:notice("HMGET Key is -->"..tostring(key))
	for idx=1,table.getn(field) do
		logger:notice("Field are -->"..tostring(field[idx]))
	end
	]]--
	local red = cacheHandle:get_redis_connection(constants.REQUEST_TYPE_READ)

	if red == nil then
		logger:error("Error While Getting Redis Connection for Request -> READ")
		return nil
	end

	local res, err = red:hmget(key, unpack(field))

	logger:info(string.format("Redis Service hmget %s %s", tostring(res), tostring(err)))

	--Response is boolean when key is not found
	if res == nil or type(res) ~= "table" then
		logger:error("Error while redis_service.hmget " .. tostring(err))
		set_keep_alive(red)
		return nil
	end

	for k,v in ipairs(res) do
		logger:debug("K is -->"..tostring(k).." V is -->"..tostring(v))
	end
	
	if type(res[1]) == "userdata" then
		logger:debug("Non Existing Key in Redis -->"..tostring(key))
		set_keep_alive(red)
		return nil
	end
	
	--Put Redis Object Back Into Connection Pool
	set_keep_alive(red)
        
	return res
end

-- Sets values of given fields in the given key's value
-- Param: key - Key
-- Param: field - List of fields for which values have to set
-- Returns: Boolean - True if the update is successful and False otherwise
function redis_service:hmset(key, field)
        local cacheHandle = nil
	cacheHandle = self.cacheHandle
	local status = true
	
	--Connect with Redis master, hence is_local has to be false
	local red = cacheHandle:get_redis_connection(constants.REQUEST_TYPE_WRITE)

	if red == nil then
		logger:error("Error While Getting Redis Connection for Request -> WRITE")
		return false
	end
        
	local res = nil
	local err = nil

	res, err = red:hmset(key, field)

	logger:debug(string.format("hmset %s %s", tostring(res), tostring(err)))
	
	if res == nil then
		logger:error("Error while redis_service.hmset " .. tostring(err))
		status = false
	else 
	    --Issue a wait call to redis, to get a write ack from the specified number of slaves
	    --The call is sync and Blocking
	    res,err = red:wait(constants.REDIS_MIN_SLAVES_ACK, constants.REDIS_WAIT_ACK_TIMEOUT)
	    logger:info(string.format("Redis Write Slaves Wait Result res = (%s), err = (%s)", tostring(res), tostring(err)))

	    if res < constants.REDIS_MIN_SLAVES_ACK  or err ~= nil then
		    logger:error("Error While Waiting for Write Ack from Slaves")
                    status = false
            end
	end

	--Put Redis Object Back Into Connection Pool
	set_keep_alive(red)

	return status
end

-- Sets value of the given key
-- Param: key - Key
-- Param: value - Value
-- Returns: Boolean - True if the update is successful and False otherwise
function redis_service:set(key, value)
        local cacheHandle = nil
	cacheHandle = self.cacheHandle
	local status = true
	
	-- Connect with Redis master, hence is_local has to be false
	local red = cacheHandle:get_redis_connection(constants.REQUEST_TYPE_WRITE)

	if red == nil then
		logger:error("Error While Getting Redis Connection for Request -> WRITE")
		return false
	end

        local res = nil
	local err = nil
	
	res, err = red:set(key, value)
	logger:debug(string.format("set %s %s", tostring(res), tostring(err)))
	
	if res == nil then
		logger:error("Error while redis_service.set " .. tostring(err))
		status = false
	else 
	    --Issue a wait call to redis, to get a write ack from the specified number of slaves
	    --The call is sync and Blocking
	    res,err = red:wait(constants.REDIS_MIN_SLAVES_ACK, constants.REDIS_WAIT_ACK_TIMEOUT)
	    logger:info(string.format("Redis Write Slaves Wait Result res = (%s), err = (%s)", tostring(res), tostring(err)))

	    if res < constants.REDIS_MIN_SLAVES_ACK  or err ~= nil then
		    logger:error("Error While Waiting for Write Ack from Slaves")
                    status = false
            end
	end
	
	--Set Keepalive so that connection is not closed
	set_keep_alive(red)

	return status
end

-- Checks the health of local redis instance
-- Using the get_redis_connection does not in true sense gurantee that all
-- the sentinels and all the hosts and slaves are reachable.
-- Currently we are piggy packing in this connection, but depending  upon the frequency of
-- check_health being invoked we can come with a wrapper and pings all the hosts of the redis
-- This kind of check would make more sense , say at a 10 Second Interval than a 1 Second Interval
-- Returns: Boolean - True if the local Redis instance is healthy and False otherwise
function redis_service:check_health()
        local cacheHandle = nil
	local red = nil
	
	cacheHandle = self.cacheHandle
	red = cacheHandle:get_redis_connection(constants.REQUEST_TYPE_READ)
	
	if red == nil then 
		logger:error("Redis Health Check Failed for Slaves")
	else 
		logger:notice("Redis Health Check Success for Slaves:  " .. tostring(red ~= nil))
	end
	
	red = cacheHandle:get_redis_connection(constants.REQUEST_TYPE_WRITE)
	if red == nil then 
		logger:error("Redis Health Check Failed for Master")
	else 
		logger:notice("Redis Health Check Success for Master" .. tostring(red ~= nil))
	end
	return red ~= nil
end

-- Checks the health of local redis instance
-- Using the get_redis_connection does not in true sense gurantee that all
-- the sentinels and all the hosts and slaves are reachable.
-- Currently we are piggy packing in this connection, but depending  upon the frequency of
-- check_health being invoked we can come with a wrapper and pings all the hosts of the redis
-- This kind of check would make more sense , say at a 10 Second Interval than a 1 Second Interval
-- Returns: Boolean - True if the local Redis instance is healthy and False otherwise
function redis_service:get_redis_topology()
        local cacheHandle = nil
	cacheHandle = self.cacheHandle
	local res = cacheHandle:update_redis_connection()
	if res == false then 
		logger:error("Update Redis Connection: Failed ".. tostring(res))
	else 
		logger:notice("Update Redis Connecection: Success:  " .. tostring(res))
	end
	return res
end

-- Execute the CL.THROTTLE COMMAND
function redis_service:clthrottle(key, value)
        local cacheHandle = nil
	cacheHandle = self.cacheHandle
	local status = true
	
	-- Connect with Redis master, hence is_local has to be false
	local red = cacheHandle:get_redis_connection(constants.REQUEST_TYPE_WRITE)

	if red == nil then
		logger:error("Error While Getting Redis Connection for Request -> WRITE")
		return false
	end

        local res = nil
	local err = nil
	
	res, err = red:clthrottle(key, value)
	logger:notice("CL.Throttle Command -->"..string.format("set %s %s", tostring(res), tostring(err)))
	
	if res == nil then
		logger:error("Error while redis_service.set " .. tostring(err))
		status = false
	else  
	    --Set Keepalive so that connection is not closed
	    set_keep_alive(red)
	end

	return res, status
end


setmetatable(redis_service_handle, redis_service)
return redis_service_handle
