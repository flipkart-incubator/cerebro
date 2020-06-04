--[[ The Module is loaded via the init_worker_by_lua_file,
--   per worker at the startup. This is used to load per worker
--   context information for blocks like LoadBalancer, Statistic,
--   Config Manager, and any other block that may require One Time 
--   Initialization
---]]

local logger = require(".lua.common.logger").new("worker_context.lua")
local timerHandler = require(".lua.timer.timer")
local statsHandler = require(".lua.stats.statscollector")
local cmgr = require(".lua.config_manager.cfgmgr")
local redisMgr = require(".lua.service.redis_connection_manager")

logger:info("Worker Context Loading Modules for ->"..ngx.worker.pid())

--Init the Modules
--Timer Module
logger:info("Init Timer Module")
timerHandler.init()

--Config Mangaer
logger:info("Init Config Manager Module")
cmgr.load()

--Init The Redis Connection Managers
logger:info("Initializing Connections to Redis Cache")
redisMgr.init()
