--[[ The Logger Module, which encapsulates
--  the logger Levels. 
--  Change of Log Level is Accomplished through Updating a 
--  Shared Memory Region. This region is read via the timer context
--  Every time if fires
--  ]]
local shared_memory = require(".lua.service.shared_memory_service")
local constants = require(".lua.common.constants")
local setmetatable = setmetatable
local format = string.format

local _M_logger = { log_level = constants.LOG_LEVEL.DEBUG }

local logger ={}
logger.__index = logger

function _M_logger.new(modname, log_level)
	if log_level == nil then
		log_level = constants.DEFAULT_LOG_LEVEL
	end
	local self = {
		modname = modname,
		log_level = log_level
	}
        --ngx.log(ngx.INFO,tostring(self.modname))
	--ngx.log(ngx.INFO,tostring(self.log_level))
	setmetatable(self, logger)
	return self
end

-- Returns the current log_level
-- Returns: current log_level
local function get_log_level(logger)
	return logger.log_level
end

--Set Log Level, called under the context
--of timer, to update any new log levels from the 
--shared memory region
function logger:set_log_level()
	
	--ngx.log(ngx.DEBUG,"Shared Memory Attempt to Get Log Level")
	local new_log_level = shared_memory.get(constants.SHARED_MEMORY_KEYS.LOG_LEVEL)
	--ngx.log(ngx.DEBUG,"Read New Log Level as -> "..tostring(new_log_level))
	
	if new_log_level == nil then
	  --      ngx.log(ngx.DEBUG,"Setting Default Log Level")
		new_log_level = constants.DEFAULT_LOG_LEVEL
	end
	self.log_level = new_log_level
	--ngx.log(ngx.DEBUG,"Updated New Log Level as -> "..tostring(new_log_level))
end

local function logmsg(logger, log_level, msg)
	local modname = logger.modname
        
	if modname == nil then
		modname = "Nil"
	end

        ngx.log(log_level,modname.." : "..msg)
end

-- Logs fatal
-- Param: message - message to be logged
-- Returns: nil
function logger:fatal(message)
	if get_log_level(self) <= constants.LOG_LEVEL.FATAL then
                logmsg(self,ngx.EMERG,message)
	end
end

-- Logs critical
-- Param: message - message to be logged
-- Returns: nil
function logger:critical(message)
	if get_log_level(self) <= constants.LOG_LEVEL.CRITICAL then
                logmsg(self,ngx.CRIT,message)
	end
end

-- Logs warning
-- Param: message - message to be logged
-- Returns: nil
function logger:warning(message)
	if get_log_level(self) <= constants.LOG_LEVEL.WARNING then
                logmsg(self,ngx.WARN,message)
	end
end

-- Logs error
-- Param: message - message to be logged
-- Returns: nil
function logger:error(message)
	if get_log_level(self) <= constants.LOG_LEVEL.ERROR then
               logmsg(self,ngx.ERR,message)
	end
end

-- Logs notice
-- Param: message - message to be logged
-- Returns: nil
function logger:notice(message)
	if get_log_level(self) <= constants.LOG_LEVEL.NOTICE then
                logmsg(self,ngx.NOTICE,message)
	end
end

-- Logs info
-- Param: message - message to be logged
-- Returns: nil
function logger:info(message)
	if get_log_level(self) <= constants.LOG_LEVEL.INFO then
                logmsg(self,ngx.INFO,message)
	end
end

-- Logs debug
-- Param: message - message to be logged
-- Returns: nil
function logger:debug(message)
	if get_log_level(self) <= constants.LOG_LEVEL.DEBUG then
                logmsg(self,ngx.DEBUG,message)
	end
end

-- Logs verbose
-- Param: message - message to be logged
-- Returns: nil
function logger:verbose(message)
	if get_log_level(self) <= constants.LOG_LEVEL.VERBOSE then
                logmsg(self,ngx.DEBUG,message)
	end
end


setmetatable(_M_logger, logger)

return _M_logger
