local logger = require(".lua.common.logger").new("request_context_builder.lua")
local constants = require(".lua.common.constants")
local util = require(".lua.common.util")
local user_status_cache = require(".lua.cache.user_status_cache")

--Get Request Content Size
--This most likley includes the headers as well, which may need to be chunked off
local function get_request_size()
	return ngx.var.request_length
end

-- Gets request uri
-- Returns: string - request uri
local function get_request_uri()
	return ngx.var.request_uri
end

-- Gets request method
-- Returns: string - request method
local function get_request_method()
	return ngx.req.get_method()
end

-- Extracts bucket name from uri
-- Param: uri - request uri
-- Returns: string - bucket name
local function get_bucket_name(uri)
	if uri == nil or uri == "" or uri == "/" then
		return nil
	end

	-- Ideally this condition should never be true
	-- To-do: Log an error message when this happens
	if string.find(uri, "/") == nil then
		return uri
	end

	return string.sub(uri, 2, string.find(uri, "/", 2) - 1)
end

-- Formats uri
-- Param: uri - Raw uri
-- Returns: string - Formatted uri
local function format_uri(uri)
	if uri == nil then
		return ""
	end
	if uri:sub(1,1) ~= "/" then
		uri = "/" .. uri
	end
	if uri:sub(#uri, #uri) ~= "/" then
		uri = uri .. "/"
	end
	return uri
end

-- Extracts access_key from Authorization header of v4 requests
-- To-do: Test properly
-- Param: authorization - Authorization header of the request
-- Returns: string - access key of the user account
local function get_access_key_from_v4_authorization(authorization)
	local start_pos = string.find(authorization, constants.CREDENTIAL_HEADER_PARAM)
	local end_pos = string.find(authorization, "/", start_pos)

	if start_pos == nil or end_pos == nil then
		return nil
	end
	
	return string.sub(authorization, start_pos + #constants.CREDENTIAL_HEADER_PARAM, end_pos - 1)
end

-- Extracts access_key from Authorization header
-- Returns: string - access key of the user account which has made the request
local function get_access_key()
	local authorization = ngx.req.get_headers()['Authorization']

	if authorization == nil then
		return nil
	end

	local access_key = get_access_key_from_v4_authorization(authorization)	

	if access_key ~= nil then
		return access_key
	end
        
	local s = string.find(authorization, ' ')
	local e = string.find(authorization, ':')

	if s == nil or e == nil then
		return nil
	end
	return authorization:sub(s + 1, e - 1)
end

-- Get request headers
-- Returns: Lua Table - Headers represented as hashtable
local function get_request_headers()
	return ngx.req.get_headers()
end

-- Checks if the migrator has made this request
-- Returns: Boolean - Returns true if the request is made by migrator and False otherwise
local function is_migrator()
	return ngx.req.get_headers()['x-is-migrator'] == 'true'
end

-- Checks if the bucket operation, Eg: GET_BUCKET, PUT_BUCKET, etc.
-- Returns: Boolean - Returns true if it's a bucket operation
local function is_bucket_operation(uri)
	local _,count = string.gsub(uri, '/', '/')
	return count == 2
end

-- Checks if it's an ACL operation
-- Returns: Boolean - Return true if it's a acl operation and False otherwise
local function is_acl_operation(uri)
	return string.find(uri, "?acl") ~= nil
end

local function get_bootstrap_cluster(akey)
        local bootstrap_cluster = nil
	
	if akey == nil then 
		bootstrap_cluster = constants.BOOTSTRAP_CLUSTER
		return bootstrap_cluster
	end
	
	local userField =  user_status_cache.getField(akey,"bootstrap_cluster")

	if userField ~= nil then 
		bootstrap_cluster =  userField[1]
	end

	return bootstrap_cluster
end

	
-- Builds request context
-- Returns: Request context object
local function get_request_context()
	local uri = format_uri(ngx.var.request_uri)
	local context = {}
	context.is_v4 = false
	context.uri = get_request_uri(uri)
	context.request_method = get_request_method()
	context.request_headers = get_request_headers()
	context.bucket_name = get_bucket_name(uri)
	context.access_key = get_access_key()
	context.migrator = is_migrator()
	context.is_bucket_operation = is_bucket_operation(uri)
	context.is_acl_operation = is_acl_operation(uri)
	context.response_code = nil
	context.request_length = get_request_size()
	--By This Time The Access Key Should Be Already Read and Filled in the Context
	context.bootstrap_cluster = get_bootstrap_cluster(context.access_key)
	return context
end

return {
	get_request_context = get_request_context
}
