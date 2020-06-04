local logger = require(".lua.common.logger").new("redis_load_balancer.lua")
local cfgmgr = require(".lua.config_manager.cfgmgr")

local tbl_sort = table.sort

local M_ctx={}
local _M={}
_M.__index = _M

function M_ctx:new(clusterName)
	logger:notice("Redis Load Balance Init Cluster --> "..clusterName)
	local self = {
		  clusterName = clusterName,
		  host_list_meta = nil,
                  sentinelLB = require(".lua.load_balancer.balancer"):new(clusterName.."sentinelLb", 5),
                  hostLB = require(".lua.load_balancer.balancer"):new(clusterName.."cacheHostLb", 5),
        }
	setmetatable(self,_M)
	return self
end

local function sort_by_hostip(a, b)
	if a.host > b.host then
            return true
	else
	    return false 
        end
end

function _M:connect_to_slaves()
    
     local currentHost = 0;
     local maxHosts = table.getn(self.host_list_meta)
     local retryCount = 0;
     local maxRetryPerHost = 1
     local r = nil
     local err = nil
     
     if maxHosts == 0 then 
	 logger:notice("Connect to Slaves .."..self.clusterName)
         self:get_host_via_sentinel(self)
     end

     local rc = require(".lua.redis.connector").new({
            connect_timeout = 500,
	    read_timeout = 500,
	    keepalive_timeout = 30000,
	})
     local hidx = -1 
     while currentHost < maxHosts do
	     local hostConnected = false
	     hidx = self.hostLB:getNextIndex(maxHosts)
             logger:notice("Current Index is --> "..tostring(hidx).."Max Hosts is --> "..tostring(maxHosts))
	     
	     if hidx > maxHosts then 
                  logger:notice("Resetting to the First Host...Change Detetced")
		  hidx = 1
	     end
	     
	     if hidx <= 0 then
		     logger:error("Invalid Index from the Load Balacner")
		     break
             end
             
	     if self.host_list_meta[hidx]["role-reported"] == "master" then
		 logger:notice("Skipping Master for Reads")
             elseif self.host_list_meta[hidx]["host_health"] == "UNHEALTHY" then
		 logger:error("Skipping Connect to UNHEALTHY Slave @ idx -->"..tostring(hidx).." IP --> "..tostring(self.host_list_meta[hidx].host).." Port --> "..tostring(self.host_list_meta[hidx].port))
	     else
	         --Retry Count Per Host
	         --Increasing the maxRetryPerHost, can cause latency increase when the Host is not Reachable
	         retryCount = 0
	         while retryCount < maxRetryPerHost do
		       logger:notice("Trying to Connect to Slave Host "..tostring(hidx))
		       self.host_list_meta[hidx].password="XXXX"
		       r, err = rc:connect_to_host(self.host_list_meta[hidx])
		       if r then 
		           logger:notice("Host Connected R Handle is "..tostring(r).." "..tostring(err).." ".."Slave Host Ip Is --> "..tostring(self.host_list_meta[hidx].host))
		           hostConnected = true
			   break
	               else
		           logger:error("Failed to Connect to Host  R Handle is "..tostring(r).." Error "..tostring(err).." ".."Slave Host Index Is --> "..tostring(hidx).."IP -->" ..tostring(self.host_list_meta[hidx].host).." Port -->"..tostring(self.host_list_meta[hidx].port))
		       end
		       retryCount = retryCount + 1
                 end
	     end	 
	     if hostConnected == true then 
                 logger:notice("Host Connected at index --> "..tostring(hidx))
                 break
             end
	     currentHost = currentHost + 1
     end

    return r, err
end

function _M:connect_to_master()
     
     local r = nil
     local err = nil

     local rc = require("lua.redis.connector").new({
            connect_timeout = 500,
	    read_timeout = 500,
	    keepalive_timeout = 30000,
	})
     
     if self.host_list_meta == nil then 
         logger:notice("Updating Hosts from Sentinels")
         self:get_host_via_sentinel(self)
     end

     logger:notice("Trying to Connect to Master ")
     local maxHosts = table.getn(self.host_list_meta)
    
     --if maxHosts == 0 then 
       --  self:get_host_via_sentinel(self)
     --end

     for hidx = 1, maxHosts do
          if self.host_list_meta[hidx]["role-reported"] == "master" then
              self.host_list_meta[hidx].password="XXXX"
	      logger:notice("Trying to Connect to Master Host "..tostring(hidx))
	      r, err = rc:connect_to_host(self.host_list_meta[hidx])
	      logger:notice("Master Handle is "..tostring(r).." "..tostring(err).." ".."Master Host IP is --> "..tostring(self.host_list_meta[hidx].host))
             end
    end

    return r, err

end

local function check_host_down_flags(host_flags)

	local result = false
        
	logger:notice("ROMISRA Host Flags --> "..tostring(host_flags)) 
	
	if host_flags == nil then
	    return true
	end

	for flag_tokens in host_flags:gmatch('[^,%s]+') do
	    logger:notice("ROMISRA FLag Tokens --> "..tostring(flag_tokens)) 
	    if flag_tokens == "s_down" then
		    result = true
		    break
            end
	    
	    if flag_tokens == "disconnected" then
		    result = true
		    break
            end
	end
	return result
end

function _M:get_host_via_sentinel()
        
        logger:notice("Updating Hosts Via Senitnel Cluster Name is --> "..self.clusterName)
        local master_name = self.clusterName
	local master = nil
	local slaves = nil
	local redis_host_list = {}
	local err = nil
	local status = true
        local hidx = -1
	local sentinelConnect = false
        local start_time = ngx.now()
        
	local sentinels = cfgmgr.getSentinelTable(self.clusterName)
        for sidx=1, #sentinels do
		logger:notice("List of Sentinels -->"..sentinels[sidx].host.." "..sentinels[sidx].port)
	end
	--logger:notice("Length of Sentinel is "..#sentinels)
	hidx = self.sentinelLB:getNextIndex(#sentinels)
	--logger:notice("Sentinel Index is "..tostring(hidx))
	local rc = require(".lua.redis.connector").new({
            connect_timeout = 500,
	    read_timeout = 500,
	    keepalive_timeout = 30000,
	    host = sentinels[hidx].host,
	    port = sentinels[hidx].port,
	    --sentinel_password='XXXXXX',
	})
        
	local redisHandle, err = rc:connect()
        logger:notice("Redis Handle"..tostring(redisHandle)..tostring(err))

	if redisHandle ~= nil then 
             logger:notice("Getting the List of Master and Slaves")
             --Get the Master
	     master, err = require(".lua.redis.sentinel").get_master(redisHandle, master_name)
             logger:notice("Redis Handle"..tostring(master)..tostring(err))
        
   	     --Get the Slaves
	     slaves, err = require(".lua.redis.sentinel").get_slaves(redisHandle, master_name)
             logger:notice("Redis Handle"..tostring(slaves)..tostring(err))

	     sentinelConnect = true
        else
           logger:error("Unable to Connect to Sentinel @ index"..tostring(hidx))
        end
       

	if sentinelConnect == true then 
	    --Coalasce the Master and Slaves Table
	    if slaves ~= nil then 
	        for idx=1,table.getn(slaves) do
	             table.insert(redis_host_list, slaves[idx])
	        end
	    end

            master["role-reported"]="master"
	    table.insert(redis_host_list, master)
	
	    tbl_sort(redis_host_list, sort_by_hostip)
	
	    for k,v in pairs(redis_host_list) do 
                logger:info(k)
	            for h,p in pairs(v) do 
                        logger:notice(h.." "..p)
		    end
	    end
	
	    --Critical Section, Shared Between the Timer Context and Request
            local redis_host_list_length = 0
            local host_list_meta_length = 0
	    local change_detected = false
	    --Compare the Cached List and the New Read list, update only if there is a change detected
	    redis_host_list_length = #redis_host_list
         
	    --Update Host Health 
	
	    for idx=1,redis_host_list_length do
                redis_host_list[idx]["host_health"] = "HEALTHY"
	        if redis_host_list[idx]["role-reported"] == "slave" then
                    local host_flags = redis_host_list[idx].flags
	            local host_down = check_host_down_flags(host_flags)
	            if host_down == true then
			logger:error("ROMISRA Marking Host UNHEALTHY")
                        redis_host_list[idx]["host_health"] = "UNHEALTHY"
	            end
	        end
	    end

            self.host_list_meta = redis_host_list
            logger:notice("Redis Hosts List Updated")
	end
        
        local end_time = ngx.now()
        local elapsed_time = end_time-start_time
        logger:notice("Start Time --> "..tostring(start_time).."End Time --> "..tostring(end_time).." Time Spent: Update Host Via Sentinel --> "..tostring(elapsed_time))
	
	return status
end

setmetatable(M_ctx,_M)
return M_ctx

