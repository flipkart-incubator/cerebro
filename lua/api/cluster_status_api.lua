local util = require(".lua.common.util")
local logger = require(".lua.common.logger").new("cluster_status_api.lua")
local constants = require(".lua.common.constants")
local cluster_status_cache = require(".lua.cache.cluster_status_cache")
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
	local num_clusters = 0
	
	for k,v in pairs(status)
	do
		-- Currently it supports updation of one account only
		if num_clusters == 1 then
			break
		end

		local cluster = k
		local cluster_details = v		
	        	
		local set_status = cluster_status_cache.set(cluster, cluster_details)
		
		logger:info("Updated CLuster Status Cache: Redis: " .. tostring(set_status))
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
	local cluster = nil

	if args ~= nil then
		cluster = args[constants.CLUSTER_STATUS_QUERY_PARAM]
	end

	if cluster ~= nil then
		local status = cluster_status_cache.getField(cluster,"epoch","version",
	           		"cluster_read_endpoint","cluster_write_endpoint",
				"cluster_state","fallback_cluster")

		response[cluster] = status
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
