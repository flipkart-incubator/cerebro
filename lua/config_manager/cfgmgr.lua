local logger = require(".lua.common.logger").new("cfgmgr.lua")
local cjson = require "cjson"
local _cmgr ={}

-- The _cmgr_table is the table that holds the parsed json file
-- This is parsed by parts into different tables that can be used throughout
-- the lifetime of the Worker. The Parse of Json file and creating the
-- individual tables from it is a INIT one time activity.
local _cmgr_table =  {}

--The Sentinel Table Holds the mapping of all the Sentinels and the
--Ports for the Redis Cluster.
local _sentinel_table={}

--The Replication Status Read from the Conf File
local replicationStatus = nil

--The API Returns a Valid Handle to a Sentinel Table if it Exists,
--otherwise it return nil
function _cmgr.getreplicationStatus()
	return tonumber(replicationStatus)
end

--The API Returns a Valid Handle to a Sentinel Table if it Exists,
--otherwise it return nil
function _cmgr.getSentinelTable(clusterName)

	if clusterName == nil then
		logger:error("Nil Cluster Name Specified, No Valid Sentinel Table")
		return nil
	end

	return _sentinel_table[clusterName]
end

function _cmgr.loadSentinelTable(sentinelTable)
	logger:debug("Type is "..tostring(type(sentinelTable)))
	logger:debug("Table Size is --> "..table.getn(sentinelTable))
	--The key value pair received here is as follows:-
	--"redis-cluster-name" : "table of sentinels"
	for k,v in pairs (sentinelTable) do 
		logger:info("Sentinel Key Name is --> "..tostring(k))
		logger:info("Sentinel Value Name is --> "..tostring(v))
		logger:info("Sentinel Key Size is --> "..table.getn(v))
		--Add to to The Table under teh config manager
		_sentinel_table[k] = v
	end
end

function _cmgr.loadClusterTable(clusterTable)
	--Parse the Decode Cluster Table and Update the same under the Config Manager
	logger:info("Size of Redis Cluster Table is -> "..tostring(tableSize))
	for k,v in pairs (clusterTable) do 
		logger:debug("Key Name is --> "..tostring(k))
		logger:debug("Type of The Key is --> "..type(k))
		logger:debug("String Value Is --> "..tostring(v))
		logger:debug("Type of the String is--> "..type(v))
		_cmgr.loadSentinelTable(v)
	end
        
	logger:debug("Sentinel Table Size is "..tostring(table.getn(_sentinel_table)))

	--Debug/Test Code for testing the Table Handles
	--[[
	test = _cmgr.getSentinelTable("redis-cluster-stage")
	logger:notice("Trying to Dump the Table redis-cluster-stage"..tostring(test))
	for idx = 1, table.getn(test) do
		logger:notice("redis-cluster-stage --> ..Host -->"..tostring(test[idx].host).."Port ..-->"..tostring(test[idx].port))
	end
	
	test = _cmgr.getSentinelTable("redis-cluster-stage-1")
	logger:notice("Trying to Dump the Table redis-cluster-stage-1"..tostring(test))
	for idx = 1, table.getn(test) do
		logger:notice("redis-cluster-stage-1 --> ..Host -->"..tostring(test[idx].host).."Port ..-->"..tostring(test[idx].port))
	end
	
	test = _cmgr.getSentinelTable("redis-cluster-stage-2")
	logger:notice("Trying to Dump the Table redis-cluster-stage-2"..tostring(test))
	if test == nil then 
		logger:notice("No Entry Exists")
	end
	]]--
end

--TBD Remove the Hard Coding From Here
function _cmgr.load()
     logger:debug("Inside Config Manager Load")
     local file = io.open("/etc/nginx/api-gateway-layer/lua/cluster.json", "r")
     local contents
     if file then 
	     contents = file:read("*a")
	     io.close(file)
     end
     _cmgr_table = cjson.decode(contents)

     for k,v in pairs(_cmgr_table) do 
	     logger:debug("Parse Decoded Json ->"..type(k).."  "..type(v)) 
	     
	     if k == 'REDIS_CLUSTER' then
	     	     logger:debug("Parse REDIS CLUSTER -->"..type(k).."  "..type(v)..tostring(k)) 
		     _cmgr.loadClusterTable(v)
             end

	     if k == 'REPLICATION' then
		     logger:debug("Replication KEY VALUE PAIIR are-->"..tostring(k)..".."..tostring(v))
		     replicationStatus = v
             end

     end
end

--Currently Called Periodically at 10 seconds interval from the timer context to read the json config
--file. Any sentinel chnage can be updated and read. The sentinels would not change very often and this
--update should most of the time result in the same config file
function _cmgr.readConfig()
      logger:debug("Disbale Calling Config Read Inside Timer Context")
      _cmgr.load()
end

function _cmgr.get_target_ip(cluster_tag, access_key)
	-- Search the Access Key Table first if this Requires a Customer Routing
	-- If not Fall back to the DEFAULT ROUTING Table
	-- Currently this is implemented as a for loop and given that this Customer User
	-- table intially will be empty it is acceptable at this point of time.
	-- We need to come up with a HASHMAP implementation of the same or a fast cache lookup
	-- if this table increase to a considerable size
	for idx=1,table.getn(_custom_user_table) do
        	if _custom_user_table[idx].KEY == access_key then 
			return _custom_user_table[idx][cluster_tag]
		end
	end
        
	--A Serach Was UnSuccesfull, return Default Value
	return _cmgr_table[cluster_tag]

end

-- Given a Cluster Name, Retrun the Read and Write EndPoints 
-- Currently this is implemented as a for loop
-- Given that the initial Cluster Would Span Not More than 2 / 3.
-- As this grows, this need to be replaced with a better efficient way, using
-- a hashmap
function _cmgr.get_cluster_detail(cluster_name)
        
	logger:debug("In Get Cluster Deatils for  cluster name --> ".. cluster_name)
	for idx=1,table.getn(_cluster_table) do
	         --logger:info("In Loop--->".._cluster_table[idx].name)
        	if _cluster_table[idx].name == cluster_name then 
	                logger:debug("Match FOund")
			logger:debug("Found R Endpoints-->".._cluster_table[idx].READ_ENDPOINT)
			logger:debug("Found W Endpoints-->".._cluster_table[idx].WRITE_ENDPOINT)
			return _cluster_table[idx].READ_ENDPOINT,_cluster_table[idx].WRITE_ENDPOINT
		end
	end
	--A Search Was Unsuccesfull, return nil.
	logger:error("Cluster Details Not Updated for Cluster Name --> "..cluster_name)
	return nil,nil
end

return _cmgr
