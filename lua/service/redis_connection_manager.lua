local logger = require(".lua.common.logger").new("redis_connection_manager.lua")
local constants = require(".lua.common.constants")
local redis_service = require(".lua.service.redis_service")

local redis_connection_manager = {}

local connectionTable = {}

function redis_connection_manager.get(connectionName)
	
	logger:notice("Recevied Request for name -->"..tostring(connectionName))
	if connectionName == nil then
		logger:error("Connection Name Not Specified")
		return nil
	end
        
	if connectionTable[connectionName] == nil then
		logger:error("Redis Connection Manager, Connection Name"..tostring(connectionName).." Handle is Nil")
	end
        
	logger:notice(tostring(connectionName).." -->Connection Manager Context -->"..tostring(connectionTable[connectionName]))
	return(connectionTable[connectionName])

end

function redis_connection_manager.init()
	--Init Connection for all the Redis Cluster Here
	local redserviceHandle1 = redis_service:new(constants.REDIS_CLUSTER_DPATH)
	connectionTable[constants.REDIS_CLUSTER_DPATH] = redserviceHandle1
	
	--Init Connection for the Rate Limiter
	local redserviceHandle2 = redis_service:new("redis-cluster-prod-test")
	connectionTable["redis-cluster-prod-test"] = redserviceHandle2
end

return redis_connection_manager



