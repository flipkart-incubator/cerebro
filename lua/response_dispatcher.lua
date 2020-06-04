local logger = require(".lua.common.logger").new("response_dispatcher.lua")
local util = require(".lua.common.util")
-- Set response header
-- Param: header - Lua Table - All header attributes represented in hashtable
-- Returns: nil
local function set_response_header(header)
	for key, value in pairs(header) do
		ngx.header[key] = value
	end
end

-- Sets response status
-- Param: status - status code
-- Returns: nil
local function set_response_status(status)
	ngx.status = status
end

-- Sets response body
-- Param: body - Response payload
-- Returns: nil
local function set_response_body(body)
	ngx.say(body)
end

-- Dispatches response
-- Param: response - Response object. It expects the object to have "header", "status" and "body" members
-- Returns: nil
local function dispatch_response(response)
	set_response_header(response.header)
	set_response_status(response.status)
	set_response_body(response.body)
end

return {
	dispatch_response = dispatch_response
}
