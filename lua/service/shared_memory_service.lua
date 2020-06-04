local shared_memory = ngx.shared.shared_memory

-- Gets value of the key from shared_memory
-- Param: key - Key for which value has to be retrieved
-- Returns: <T> - Value
local function get(key)
	return shared_memory:get(key)
end

-- Sets value for the given key
-- Param: key - Key
-- Param: value - Value
-- Returns: Boolean - True if it succeeds and False otherwise
local function set(key, value)
	if key == nil or value == nil then
		return false
	end

	return shared_memory:set(key, value)
end

return {
	get = get,
	set = set
}
