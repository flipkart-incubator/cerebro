local util = require(".lua.common.util")
local logger = require(".lua.common.logger").new("replication_status_api.lua")
local constants = require(".lua.common.constants")
local replication_status_cache = require(".lua.cache.replication_status_cache")
local redis_service = require(".lua.service.redis_service")

--~ Sends success response to caller
local function send_success_response()
	util.send_response(200)
end

--~ Sends failure response to the caller
local function send_failure_response()
	util.send_response(400)
end

--~ Process PUT API call
local function process_put()
	local status_raw = util.get_request_body()
	local status = util.parse_json(status_raw)
	local num_buckets= 0
	
	for k,v in pairs(status)
	do
		-- Currently it supports updation of one account only
		if num_buckets == 1 then
			break
		end

		local bucket = k
		local bucket_details = v		
	        	
		local set_status = replication_status_cache.set(bucket, bucket_details)
		
		logger:info("Updated Replication Status Cache: Redis: " .. tostring(set_status))
		logger:info(string.format("Set status from Redis: %s", tostring(set_status)))

		if set_status == false then
			send_failure_response()
		end
	end
	send_success_response()
end

-- Process GET API call
local function process_get()
	local args = util.get_query_params()
	local response = {}
	local user = nil

	if args ~= nil then
		user = args[constants.REPLICATION_STATUS_QUERY_PARAM]
	end

	if user ~= nil then
		local status = replication_status_cache.getField(user,"epoch","version",
					"repl_mode","read_cluster","write_cluster")
		response[user] = status
	else
		-- Retrieves status of all cluster
		-- To-do: Implement this
		send_failure_response()
	end

	util.send_response(200, util.convert_to_json(response))
end

--To-do: If any exception happens while processing API call, we have to send 5xx response with exception details.

local request_method = util.get_request_method()

if request_method == "PUT" or request_method == "POST" then
	process_put()
else
	process_get()
end
