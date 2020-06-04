local logger = require(".lua.common.logger").new("local_cache_manager.lua")
local constants = require(".lua.common.constants")
local lrucache = require(".lua.service.lrucache")
local local_cache={}
local cache={}
cache.__index = cache

function local_cache:new(cacheName)
	logger:notice("Local Connection Manager Received a New Request for --> "..tostring(cacheName))
	if cacheName == nil then
		logger:error("Empty Cache Connection Name Specified, cannot Instantiate Object for Cache Handle")
		return nil
	end

        local self = {
		     --Construct the Appropriate Url for masters and all hosts
		     cacheName = cacheName,
		     --cacheTable = {}
		     cacheTable = lrucache.new(100000)
        }
	setmetatable(self,cache)
	return self
end

--API to Insert a new Key Value Pair to the Local Cache Table
--The Key is the string used to Index to  the Lua Table.
--The Value itself is a Lua Table, which represents a combination of tuples
function cache:insert_entry(k,v)
	logger:info("Inserting Entry Into Local Cache for"..tostring(k).."For Value -->"..tostring(v))
	if k == nil then
		logger:error("Empty Key Cannot Insert")
		return false
	end
	
	if v == nil or type(v) ~= "table" then 
		logger:error("Empty Value, or Value Tuple is  not a Table")
		return false
	end
	self.cacheTable:set(k,v)
	return true
end

--API to Delete a a Cache Entry
function cache:delete_entry(k)
	logger:info("Deleting Entry from Local Cache for"..tostring(k))
	if k == nil then
		logger:error("Empty Key Cannot Delete -->"..tostring(k))
		return false
	end
	self.cacheTable:delete(k)
	return true
end

--API to Return a Particular Value for a Give Key Tuple. 
--There are 2 indirections here, the 'k' filed searches the respective key, and the 'v'
--Searches the corresponding tuple for the particular key
function cache:get_entry(k,v)
	--self.cacheTable:get(k]["modts"] = "7890123"
	local keyHandle = self.cacheTable:get(k)
	if keyHandle == nil then
		logger:info("Local Cache Empty for Key-->"..tostring(k))
		return nil
	end
	logger:info("Key Handle is -->"..tostring(keyHandle).."Value is -->"..tostring(keyHandle[v]))
	return keyHandle[v]
	--return self.cacheTable[k][v]
end

function cache:exists(k)
	local keyHandle = self.cacheTable:get(k)
	return keyHandle
end

--Function to Print the Basic Details of the Cache Handle
function cache:print_details()
	logger:notice("Cache Name is -->"..tostring(self.cacheName))
	--logger:notice("Cache Table Handle is"..tostring(self.cacheTable))
	--logger:notice("Cache Table Size is"..tostring(table.getn(self.cacheTable)))
end

--Function to Print the Basic Details of the Cache Stored
function cache:print_cachetable_details(key)
	--local cacheSize = #self.cacheTable
	--self.cacheTable:walk()
	logger:debug("Cache Name is -->"..tostring(self.cacheName))
	logger:debug("Actual Cache Size is"..tostring(self.cacheTable:getSize()))
	for k,v in pairs(self.cacheTable:get(key)) do
		logger:debug("Value of K2 -->" ..tostring(k) .. "Value of V2 -->" ..tostring(v))
	end
end


setmetatable(local_cache,cache)
return local_cache
