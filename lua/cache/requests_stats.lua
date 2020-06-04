local constants = require(".lua.common.constants")
local logger = require(".lua.common.logger").new("request_status_cache.lua")
local util = require(".lua.common.util")
--local redis_service = require(".lua.service.redis_connection_manager").get(constants.REDIS_CLUSTER_DPATH)

--~ Sets Key Value Pair for Bucket 
local function set(key, value)
	--Currenlty it is seen that in the timer context the module is not preserved
	--For example reds_service, that is why this is reference at every call.
	--Since  this API is being called in the timer context, the performance impact is not \
	--the bottlneck. Need to figure the way of persisting states in timer contexts
	
	local redis_service = require(".lua.service.redis_connection_manager").get(constants.REDIS_CLUSTER_DPATH)

	--TBD Should we get the Old state if present and then log the current v/s previos
	--state. For now we are logging the new successfull state
       	
	if value == nil then 
		logger:error("Nil Value Passed for Setting in Request Stats Cache")
		return false
	end

	local result = true 
	value["version"] = 1.0

	result = redis_service:hmset(key, value)
	result = redis_service:hincrby(key, "epoch", 1)
	return result
end

return {
	set = set,
}
