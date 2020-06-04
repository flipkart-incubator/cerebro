local json = require "cjson"
local constants = require(".lua.common.constants")
local logger = require(".lua.common.logger").new("util.lua")

-- Gets method constant
-- Param: method_str - Request method as string
-- Returns: ngx.<METHOD>
local function get_method_constant(method_str)
	return constants.METHOD_CONSTANTS[method_str]
end

-- Checks if the request is a Read Request
-- Param: method - request method
-- Returns: Boolean - If the request is a read request or not
local function is_read_request(method)
	return method == "GET" or method == "HEAD"
end

-- Checks if the request is a Write Request
-- Param: method - request method
-- Returns: Boolean - If the request is a read request or not
local function is_write_request(method)
	return method == "PUT" or method == "POST"
end

-- Checks if the request is a Delete Request
-- Param: method - request method
-- Returns: Boolean - If the request is a read request or not
local function is_delete_request(method)
	return method == "DELETE"
end


-- Converts dictionary to string
-- Param: dict - Dictionary which has to be stringified
-- Returns: string - Stringified dictionary
local function stringify_dict(dict)
	if dict == nil then
		return "nil"
	end

	local str = ""
	for key, value in pairs(dict) do
		str = str .. tostring(key) .. " : " .. tostring(value) .. "\n"
	end
	return str
end

-- Load json file
-- Param: file_name - File which has to be loaded
-- Returns: JSON (Lua Table) - File content in JSON (Lua Table) format.
-- Note: This raises exception if the file has invalid JSON
local function load_json(file_name)
	local file = io.open(file_name, "rb")
	local content = file:read "*a"
	file:close()
	-- ngx.log(ngx.DEBUG, "content " .. tostring(content))
	local content_json = json.decode(content)
	return content_json
end

-- Parse JSON
-- Param: raw_json - json as string
-- Returns: JSON (Lua Table) - raw_json in table format
-- Note: This raises exception if the content is in invalid JSON
local function parse_json(raw_json)
	return json.decode(raw_json)
end

-- Get current timestamp
-- Returns: Date - current timestamp
local function get_timestamp()
	return ngx.now()
end

-- Get elapsed time
-- Param: start_time
-- Returns: Integer - Time difference in ms
local function get_elapsed_time(start_time)
	return (ngx.now() - start_time) * 1000
end

-- Gets the request body
-- Returns: string - request payload
local function get_request_body()
        ngx.req.read_body()
        return ngx.req.get_body_data()
end

-- Write content to file
-- Param: file_name - Name of the file which has to be written
-- Param: content - content that has to be written
-- Returns: nil
local function write_to_file(file_name, content)
	local file_pointer = io.open(file_name, "w")
	io.output(file_pointer)
	io.write(content)
	io.close(file_pointer)
end

-- Converts Lua Table to JSON string
-- Param: table
-- Returns: string
local function convert_to_json(table)
	return json.encode(table)
end

-- Send required response
-- Param: code - status code of the response
-- Returns: nil
local function send_response(code, payload)
	if payload == nil then
		ngx.exit(code)
	else
		ngx.status = code
		ngx.say(payload)
	end
end

-- Returns request query parameters
-- Returns: Lua Table - Query params as table
local function get_query_params()
	return ngx.req.get_uri_args()
end

-- Returns request method
-- Returns: string - Request method as string
local function get_request_method()
	return ngx.req.get_method()
end

-- Checks if the status code is a success code
-- Param: status_code - Status code as number
-- Returns: Boolean - True if status is a success code and False otherwise
local function is_success_code(status_code)
	return math.floor(status_code/100) == 2
end

return {
	stringify_dict = stringify_dict,
	get_method_constant = get_method_constant,
	load_json = load_json,
	parse_json = parse_json,
	to_date = to_date,
	is_read_request = is_read_request,
	is_write_request = is_write_request,
	is_delete_request = is_delete_request,
	get_timestamp = get_timestamp,
	get_elapsed_time = get_elapsed_time,
	get_request_body = get_request_body,
	write_to_file = write_to_file,
	convert_to_json = convert_to_json,
	send_response = send_response,
	get_query_params = get_query_params,
	get_request_method = get_request_method,
	is_success_code = is_success_code
}
