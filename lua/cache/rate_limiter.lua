local constants = require(".lua.common.constants")
local logger = require(".lua.common.logger").new("rate_limiter.lua")
local util = require(".lua.common.util")
local redis_service = require(".lua.service.redis_service")
local redis_connect = require(".lua.redis.connector")
--local _smctx = require(".lua.sm.state_transistions")

local function check_rate_limit(context)
	if context.access_key == nil then 
		logger:info("Anonymnous Request, Get the Access Key from the Bucket Name via lookup")
		--TBD
		return 200
	end
	logger:info("Checking Rate Limit for access key --> "..tostring(context.access_key))
	local res = redis_service.call()
	logger:info("Result is --->"..tostring(res))
	if (tostring(res[1])) == "1" then 
		return 503
	else 
		return 200
	end
end

return {
	check_rate_limit = check_rate_limit
}
