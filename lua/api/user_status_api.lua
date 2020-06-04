local util = require(".lua.common.util")
local logger = require(".lua.common.logger").new("user_cache_api.lua")
local constants = require(".lua.common.constants")
local user_status_cache = require(".lua.cache.user_status_cache")

--~ Sends success response to caller
local function send_success_response()
	util.send_response(200)
end

--~ Sends failure response to caller
local function send_failure_response()
	--To-do: Add reason for rejection
	util.send_response(400)
end

--~ Process the PUT API call
local function process_put()
	local user_status_raw = util.get_request_body()
	local user_status = util.parse_json(user_status_raw)
	local num_accounts = 0
	
	for k,v in pairs(user_status)
	do
		-- Currently it supports updation of one account only
		if num_accounts == 1 then
			break
		end

		local account = k
		
		local set_status = user_status_cache.set(account, v)

		logger:info("Updated User Status Cache: Redis: " .. tostring(set_status))

		if set_status == false then
			send_failure_response()
		end
	
	end

	send_success_response()
end

-- Processes GET API calls
local function process_get()
	local args = util.get_query_params()
	local response = {}
	local account = nil

	if args ~= nil then
		account = args[constants.USER_STATUS_QUERY_PARAM]
	end
	-- Retrieve status of requested account only
	if account ~= nil then
		local status = user_status_cache.getField(account,"epoch","version","target_write_cluster")
		response[account] = status
	else
		-- Retrieve status of all accounts
		-- To-do: Implement this
		send_failure_response()
	end

	logger:info("Response: " .. util.convert_to_json(response))
	util.send_response(200, util.convert_to_json(response))
end

--To-do: In case, if any exception happens while processing API request (other than parsing errors, which should be counted as BadRequest), send 5xx error with exception details. In short, a global try-catch handler has to be added here.

local request_method = util.get_request_method()

if request_method == "PUT" or request_method == "POST" then
	process_put()
else
	process_get()
end
