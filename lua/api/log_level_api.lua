--[[
It stores data in shared_memory which is an in-memory hash table.

DATA FORMAT:
KEY: level 
VALUE: desired log level

Example:
Key: level
Value: debug

Note: This is not persisted. Whenever the process is started the log-level is reset to constants.DEFAULT_LOG_LEVEL
]]--

local util = require(".lua.common.util")
local logger = require(".lua.common.logger").new("log_level_api.lua")
local constants = require(".lua.common.constants")
local shared_memory = require(".lua.service.shared_memory_service")

-- Sends success response
-- -- Returns: nil
local function send_success_response()
	ngx.exit(200)
end

-- Sends failure response
-- -- Returns: nil
local function send_failure_response()
	ngx.status = 500
	ngx.say("Bad input format")
end

-- Returns numerical equivalent of the given log_level
-- -- Param: log_level - Log level as string
-- -- Returns: Numerical equivalent of the given log_level
local function get_log_level(log_level)
	if log_level == constants.LOG_LEVEL_API.FATAL then
		return 5
	elseif log_level == constants.LOG_LEVEL_API.ERROR then
		return 4
	elseif log_level == constants.LOG_LEVEL_API.WARNING then
		return 3
	elseif log_level == constants.LOG_LEVEL_API.INFO then
		return 2
	elseif log_level == constants.LOG_LEVEL_API.DEBUG then
		return 1
	else
		--Invalid log level is passed
		return -1
	end
end

-- Processes API call
-- Returns: nil
local function process()
	local log_level_raw = util.get_request_body()
	local log_level = util.parse_json(log_level_raw)
	
	for k,v in pairs(log_level)
	do
		local key = k
		local value = get_log_level(v)

		if key ~= constants.LOG_LEVEL_API_KEY or value == -1 then
			send_failure_response()
			return
		end
		
		local set_status = shared_memory.set(constants.SHARED_MEMORY_KEYS.LOG_LEVEL, value)

		logger:debug(string.format("Log level API cache backend set status: %s", tostring(set_status)))

		if set_status == false then
			send_failure_response()
			return
		end
	end

	send_success_response()
end

-- To-do: Return 5xx response when some exception happens with formatted error message.
process()
