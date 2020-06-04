local logger = require(".lua.common.logger").new("request_dispatcher.lua")
local util = require(".lua.common.util")
local json = require "cjson"

-- Returns request url
-- Param: req - Request object
-- Returns: string - request url
local function get_request_url(req)
	return req.location .. req.uri
end

-- Returns request options
-- Param: req - Request object
-- Returns: Lua table - Dictionary containing method and body, and copy
-- of the ngx.var.* to the subrequest
local function get_request_options(req)
	local options = { 
		method = util.get_method_constant(req.method),
		body = req.body,
		share_all_vars = true
	}
	return options
end

-- Sets request headers
-- Param: req - Request object
-- Returns: nil
local function set_request_headers(req)
	for attr, value in pairs(req.headers) do
		if attr ~= nil then
			ngx.req.set_header(attr, value)	
		end
	end
end

-- Dispatches the request
-- Param: req - Request object
-- Returns: res - ngx response object. It has status, header, body and truncation attributes.
local function send_request(req)
	set_request_headers(req)

	logger:debug("Request Before Location Capture ---> "..get_request_url(req))
	local res = ngx.location.capture(get_request_url(req), get_request_options(req))
	return res
end

return {
	send_request = send_request
}
