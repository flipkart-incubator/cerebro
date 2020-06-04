--[[ The Timer Module used for Invoking Periodic Tasks.
--Currently Supports a single timer, but can be easily extended to 
--support multiple timers.]]

local constants = require(".lua.common.constants")
--Common Handle to Get the Cache Handle for Redis
local cacheHandle = require(".lua.cache.cache_common")
local configHandle = require(".lua.config_manager.cfgmgr")
local statshandle = require(".lua.stats.statscollector")
local logger = require(".lua.common.logger").new("timer.lua")
local timer_ctx = {}

local timer_restart
local timer_restart_1_min
local timer_restart_10_min

local _timer_info =  {
        delay = constants.TIMER_PERIOD, -- in seconds
	workerpid = ngx.worker.pid()
}

local _timer_info_1_min =  {
        delay = constants.TIMER_PERIOD_1_MIN, -- in seconds
	workerpid = ngx.worker.pid()
}

local _timer_info_10_min =  {
        delay = constants.TIMER_PERIOD_10_MIN, -- in seconds
	workerpid = ngx.worker.pid()
}

local function timerHandlerInternal()
       	
	local status = true
	local error_message = nil
	--TBD pcall per API instead of timerHandlerInternal
	logger:debug("Firing Timer for Worker -> ".._timer_info["workerpid"])
        
	--Read the Config File
	logger:info("Reading Config File for timer id --> ".._timer_info["workerpid"])
	status, error_message = pcall(configHandle.readConfig)

	if status == false then 
		logger:error("Failed Config Read Duing Timer Event, Message-->"..tostring(error_message))
	end
	
	--Do the Periodic House Keeping Here
	--Currenlty there ia single 10 second timer. This can be multliplexed into multiple
	--timers based upon the requriements.

	logger:notice("Checking Cache Health for -> ".._timer_info["workerpid"])
	--Perform Cache Health Check
	status, error_message = pcall(cacheHandle.check_health,constants.REDIS_CLUSTER_DPATH)
	
	if status == false then 
		logger:error("Failed Cache Health Check"..tostring(error_message).." "..tostring(status))
	end

	logger:info("Updating Log Level for -> ".._timer_info["workerpid"])
	--Update Log Level
	--[[
	status, error_message = pcall(logger:set_log_level)
	if status == false then 
		logger:error("Failed To Update Log Level"..tostring(error_message))
	end
	]]--
end


local function timerHandler()

	timerHandlerInternal()
	--Finally Restart The Timer
	timer_restart()
end

local function timerHandler_1_min()
	local status = true
	local error_message = nil

	--Use Pcall While Calling the Timer Handler Events
	--This is required to make sure that at the end of the event, in either
	--pass or fail case the timer is started back.
	--These are one short timers taht needed to be started.
	--Recurring Timers are not used, as they can potentially cause pile up of timer
	--events under degraded state
	logger:notice("Timer Fired 60 Seconds.")
	status, error_message = pcall(cacheHandle.get_redis_state,constants.REDIS_CLUSTER_DPATH)
	logger:notice("Status Get Redis State.."..tostring(status).." ".."RC .."..tostring(error_message))
	--Finally Restart The Timer
	timer_restart_1_min()
end

local function timerHandler_10_min()
	local status = true
	local error_message = nil

	--Use Pcall While Calling the Timer Handler Events
	--This is required to make sure that at the end of the event, in either
	--pass or fail case the timer is started back.
	--These are one short timers taht needed to be started.
	--Recurring Timers are not used, as they can potentially cause pile up of timer
	--events under degraded state
	status, error_message = pcall(statshandle.update_stats_remote)
	status, error_message = pcall(configHandle.readConfig)
	--Finally Restart The Timer
	timer_restart_10_min()
end


--Lua Magic for Forward Declarations
--Api Callled for Subsequent Restarts of the Timer. The Restart 
--is randomized (Emulating a Jitter)
timer_restart = function()
	local ok = nil
	local err = nil
        local sleepFor = nil

	--Get the RandoM Number and convert it into Seconds Equivalent
	--Multiplication by .001 expresses the seconds in terms of milliseconds
	sleepFor = math.random(1,constants.TIMER_JITTER)
        sleepFor = sleepFor * 0.001
	ngx.sleep(sleepFor)

	ok, err = ngx.timer.at(tonumber(_timer_info["delay"]),timerHandler)
        
	if not ok then 
		ngx.log(ngx.ERR, "Failed to Create the Timer: ", err)
		--TBD : Restart The Worker
		return
	end
	logger:debug("Timer Restarted for Worker ->".._timer_info["workerpid"])
end

--Lua Magic for Forward Declarations
--Api Callled for Subsequent Restarts of the Timer. The Restart 
--is randomized (Emulating a Jitter)
timer_restart_10_min = function()
	local ok = nil
	local err = nil
	ok, err = ngx.timer.at(tonumber(_timer_info_10_min["delay"]),timerHandler_10_min)
        
	if not ok then 
		ngx.log(ngx.ERR, "Failed to Create the Timer: ", err)
		--TBD : Restart The Worker
		return
	end
	statshandle.print_stats_details()
	logger:info("Timer 10 Min Started for Worker ->".._timer_info_10_min["workerpid"])
end

--Lua Magic for Forward Declarations
--Api Callled for Subsequent Restarts of the Timer. The Restart 
--is randomized (Emulating a Jitter)
timer_restart_1_min = function()
	local ok = nil
	local err = nil
	ok, err = ngx.timer.at(tonumber(_timer_info_1_min["delay"]),timerHandler_1_min)
        
	if not ok then 
		ngx.log(ngx.ERR, "Failed to Create the 60 Seconds Timer: ", err)
		--TBD : Restart The Worker
		return
	end
	logger:notice("Timer 60 Seconds Restarted for Worker ->".._timer_info_1_min["workerpid"])
end

        

--Timer Start API to be Called only in Context of 
--init_worker_by_lua
local function timer_start()
	local ok = nil
	local err = nil
        
	
	logger:info("Starting Timer  for Worker ->".._timer_info["workerpid"])
	ok, err = ngx.timer.at(tonumber(_timer_info["delay"]),timerHandler)
        
	if not ok then 
		ngx.log(ngx.ERR, "Failed to Create the Timer: ", err)
		--TBD : Restart The Worker
		return
	end
	logger:info("Timer Started for Worker ->".._timer_info["workerpid"])
end

--Timer Start API to be Called only in Context of 
--init_worker_by_lua
local function timer_start_10_min()
	local ok = nil
	local err = nil
        
	
	logger:notice("Starting 10 Minute Timer  for Worker ->".._timer_info_10_min["workerpid"])
	ok, err = ngx.timer.at(tonumber(_timer_info_10_min["delay"]),timerHandler_10_min)
        
	if not ok then 
		ngx.log(ngx.ERR, "Failed to Create the Timer: ", err)
		--TBD : Restart The Worker
		return
	end
	logger:notice("Timer Started for Worker ->".._timer_info_10_min["workerpid"])
end

--Timer Start API to be Called only in Context of 
--init_worker_by_lua
local function timer_start_1_min()
	local ok = nil
	local err = nil
        
	
	logger:notice("Starting 60 Seconds Timer  for Worker ->".._timer_info_1_min["workerpid"])
	ok, err = ngx.timer.at(tonumber(_timer_info_1_min["delay"]),timerHandler_1_min)
        
	if not ok then 
		ngx.log(ngx.ERR, "Failed to Create the Timer: ", err)
		--TBD : Restart The Worker
		return
	end
	logger:notice("Timer Started for 60 Seconds Worker ->".._timer_info_1_min["workerpid"])
end



--Init Function, where the Initial Timers are Started
function timer_ctx.init()
	ngx.log(ngx.INFO, "Init Timer for Worker - > ".._timer_info["workerpid"])
	timer_start()
	timer_start_1_min()
	timer_start_10_min()

end

return timer_ctx
