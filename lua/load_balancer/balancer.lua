--[[ The Module has the following working:-
--   1. Every Nginx Worker will Initialize this at the Per Worker Level
--   2. Eg 10 workers, 10 iniialization each at worker level, each having its own copy
--   3. Each worker will seed a random value during initialization 
--   4. The Seed will be passed to a randomized function
--   5. The ouptut of the randomized function will be normalized to get 2 values
--          5.1 The starting Index (k)
--          5.2 The Direction (Clockwise/ Counter Clockwise) (d)
--   6.Here k is the index in the target array (which consists of clients to which
--      the requests need to be sent)
--   7.d would determine the direction we need to traverse the array
--   8.Each worker will call this module across all the boxes
--   9.Given that all the clients are up and in a healthy state, the randomized
--     approach would roughly balance the load across al the clients
--   10.In case of a unhealty client the fallback is to get the next healthy client 
--      in the respective direction
--   11. Currently The Module would just return the next 'k' and 'd' 
--   12.The mapping of indexes to actual hosts would be the responsibility of the caller 
--       module
--
]]

local logger = require(".lua.common.logger").new("balancer.lua")
local lbalancer_ctx={}
local _lbalancer={}
_lbalancer.__index = _lbalancer


local function printdetails(lb)
        logger:info("Load Balacner Name is .."..tostring(lb.name))
        logger:info("Load Balacner Worker Id .."..tostring(lb.workerpid))
	logger:info("Load Balacner Start Idx.."..tostring(lb.startIdx))
	logger:info("Load Balancer Direction %s"..tostring(lb.lookupDir))
	logger:info("Load Balacner Max Idx %s"..tostring(lb.maxIdx))
end

local function init(lb,numhosts)
	
	logger:info("Initializing Load Balancer -->" ..tostring(lb.name)..tostring(" with hosts = ".. tostring(numhosts)))
	lb.maxIdx = numhosts
        math.randomseed(lb.workerpid)
	-- Yes, this is not a typo LUA, addresses array from 1 as compared to traditional 0
	lb.startIdx = math.random(1,lb.maxIdx)
	
	if lb.startIdx % 2 == 0 then
		lb.lookupDir = 1
	else 
		lb.lookupDir = -1
	end

	printdetails(lb)
end

function lbalancer_ctx:new(lbName, numhosts)
	logger:notice("Received New LB Request for -->"..tostring(lbName).." For Size is -->"..tostring(numhosts))
	if numhosts <= 0 then
		logger:error("Invalid Size Request for Creation of LB, LB Create Fail")
		return nil
	end

	local self = {
		name = lbName,
		lookupDir = 0,
		startIdx = 0,
		maxIdx= 0,
		workerpid=ngx.worker.pid()
	}

	init(self, numhosts)	
	setmetatable(self,_lbalancer)
	return self
end


function _lbalancer:getNextIndex(numHosts)

	logger:notice("LoadBalancer Handle is -->"..tostring(self.name).."Received numHosts as -->"..tostring(numHosts))
	local resIdx = 0
	local lookupDir = tonumber(self.lookupDir)
	local startIdx  = tonumber(self.startIdx)
        local maxIdx = tonumber(self.maxIdx)
        local workerpid = tonumber(self.workerpid)
        local hostCount = tonumber(numHosts)
        
	logger:info("LoadBalancer Received HostCount as -->"..tostring(hostCount))

	if hostCount <= 0  then
		logger:error("Load Balancer Host Count Invalid, Bailing Out")
		return -1;
	end
        
	--If Host Count is Changed, Resize it.
	if hostCount ~= maxIdx then
		logger:notice(tostring(self.name).." --> Numbers of Hosts Changed Received for LoadBalancer ".."Current --> "..tostring(maxIdx).. "New Hosts --> "..tostring(hostCount))
                init(self,hostCount)
		logger:notice(tostring(self.name).." Updated LoadBalancer ".."Current --> "..tostring(maxIdx).. "New Hosts --> "..tostring(numHosts))
                maxIdx = self.maxIdx
		startIdx = 1
	end
	
        
         logger:notice(tostring(self.name).." Startidx "..tostring(startIdx).."Residx --> "..tostring(resIdx).. "Maxidx --> "..tostring(maxIdx))
	-- Left to Right
	if lookupDir == 1 then
		if startIdx == maxIdx then
			resIdx = maxIdx
			self.startIdx = 1
		else 
			resIdx = startIdx
			startIdx = startIdx + 1
			self.startIdx = startIdx
		end
	--Right to Left
	elseif lookupDir == -1 then
		if startIdx == 1 then
			resIdx = startIdx
			self.startIdx = maxIdx
		else
			resIdx = startIdx
			startIdx = startIdx - 1
			self.startIdx = startIdx
		end
	end
        
	logger:notice(tostring(self.name).." --> Next Index Returned is "..tostring(resIdx).."For Pid"..tostring(workerpid).."Direction is"..tostring(lookupDir))
	return resIdx
end


setmetatable(lbalancer_ctx,_lbalancer)
return lbalancer_ctx
