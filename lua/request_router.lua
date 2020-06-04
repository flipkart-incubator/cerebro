local request_context_builder = require(".lua.request_context_builder")
local gateway_new = require(".lua.gateway_new")
local util = require(".lua.common.util")
local logger = require(".lua.common.logger").new("request_router.lua")

-- Request processing starts here..
-- Build request context


local request_context = request_context_builder.get_request_context()

--Print the Incoming Request Here
logger:debug(string.format("Request Builder\nuri: %s\nrequest_method: %s\nbucket_name: %s\naccess_key: %s\nis_bucket_operation: %s\nis_acl_operation: %s\n", request_context.uri, request_context.request_method, request_context.bucket_name,request_context.access_key, tostring(request_context.is_bucket_operation), tostring(request_context.is_acl_operation)))

local status, error_message = pcall(gateway_new.route_request, request_context)

-- If some exception happens, invoke callback method
if status == false then
	logger:error(string.format("Exception When Forwarding Request, Falling Back to Original Cluster %s\n",request_context.uri))
	gateway_new.fallback(error_message, request_context)
end

