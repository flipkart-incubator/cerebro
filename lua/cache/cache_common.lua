local constants = require(".lua.common.constants")
local logger = require(".lua.common.logger").new("cache_common")

local function check_health(clusterName)
	local redis_service = require(".lua.service.redis_connection_manager").get(clusterName)
	logger:info("Cache Health Check Invoked.."..tostring(redis_service))
	return redis_service:check_health()
end

local function get_redis_state(clusterName)
	local redis_service = require(".lua.service.redis_connection_manager").get(clusterName)
	logger:info("Get Redis Status Invoked.."..tostring(redis_service))
	return redis_service:get_redis_topology()
end

return {
	check_health = check_health,
	get_redis_state = get_redis_state
}

