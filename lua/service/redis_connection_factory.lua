local logger = require(".lua.common.logger").new("redis_connection_factory.lua")
local constants = require(".lua.common.constants")
--local rc = require(".lua.redis.connector")
--local rc = require(".lua.redis_load_balancer")
local cfgmgr = require(".lua.config_manager.cfgmgr")

local redis_cache={}
local cache={}
cache.__index = cache

function redis_cache:new(clusterName)
	--TBD Fix this Hardcoding
	local pwd = "GvC98!nHy0xV"
	logger:notice("Redis Connection Factory Received a New Request for --> "..tostring(clusterName))
	if clusterName == nil then
		logger:error("Empty Cluster Name Specified, cannot Instantiate Object for Cache Handle")
		return nil
	end
	--Resolve Senitnel Table by the ClusterName of the Redis Accordingly
	local sentinels = cfgmgr.getSentinelTable(clusterName)
	logger:notice("Sentinel Table in Redis Cache Init is"..tostring(sentinels))
        local self = {
		     --Construct the Appropriate Url for masters and all hosts
		     --clusterName = clusterName,
		     --masterUrl  = "sentinel://"..pwd.."@"..clusterName..":m/",
		     --anyHostsUrl = "sentinel://"..pwd.."@"..clusterName..":a/",
		     sentinelTable = sentinels,
                     rc = require(".lua.redis_load_balancer"):new(clusterName),
        }
	setmetatable(self,cache)
	return self
end

--Function to Print the Basic Details of the Redis Handle
function cache:print_details()
	logger:notice("Cluster Name is -->"..tostring(self.clusterName))
	logger:notice("Master Url is -->"..tostring(self.masterUrl))
	logger:notice("Any Hosts Url  is -->"..tostring(self.anyHostsUrl))
end

--Function to Map the Appropriate Sentinel Table to the Redis Handle.
--The Sentinel Table is loaded  via the Config Manager.
local function prepare_sentinel_data(self)

    --TBD This is not required, as the table can be directly passed to the redis connector interface below
    --Once Tested Remove this part of the code (The commented One)
     --[[local tableidx = table.getn(self.sentinelTable)
     local sentinelData = {}
     for idx=1, tableidx do
	     local hostData = {host = self.sentinelTable[idx].host, port = self.sentinelTable[idx].port}
	     logger:notice("Sentinel For Contact are "..tostring(self.sentinelTable[idx].host))
             table.insert(sentinelData, hostData)
     end
     return sentinelData
     ]]--

     return self.sentinelTable
end

-- Prepare Redis connection
-- Returns: Redis connection object
local function prepare_redis_connection(self, reqtype)
       local rHandle = nil
       local err = nil
       if reqtype == constants.REQUEST_TYPE_READ then
            rHandle, err = self.rc:connect_to_slaves()
       else 
            rHandle, err = self.rc:connect_to_master()
       end
       
       return rHandle
end

-- Returns redis connection with appropriate redis instances
-- Returns: redis connection
function cache:get_redis_connection(reqtype)
	local redis_connection = prepare_redis_connection(self, reqtype)
	return redis_connection
end

-- Update Redis Toplogy
-- Returns: Redis connection object
function cache:update_redis_connection()
       local rHandle = nil
       local err = nil
       rHandle, err = self.rc:get_host_via_sentinel()
       return rHandle
end


setmetatable(redis_cache,cache)
return redis_cache
