--[[ The Module has the following working:-
--   1. Every Nginx Worker will Initialize this at the Per Worker Level
--   2. Eg 10 workers, 10 iniialization each at worker level, each having its own copy
--   3. Every Worker Will Log Relevant stats for each request it recevies
--   4. These can be written to a external DB Store (File/Cache) 
--   5. The trigger to write these to the DB Store would be either Timer Driven or at After
--       certain set thresholds amounts of requests have been processed
]]

local logger = require(".lua.common.logger").new("statscollector.lua")
local lcache = require(".lua.service.local_cache_manager"):new("local_stats_cache")
local stats_cache = require(".lua.cache.requests_stats")
local access_key_list = {}
local _statscollector ={}
local hostname = nil
local stats_key_prefix = nil


local _perReq_info = {
       reqId = 0,
       startTime = 0,
       endTime = 0
}

local _statscollector_info =  {
	fallback_count = 0,
	readfallback_count = 0,
	readreq_count = 0,
	writereq_count = 0,
        delreq_count = 0,
        setacl_count = 0,
	rejectreq_count = 0,
	src_cluster_req_count = 0,
	dst_cluster_req_count = 0,
	globalreqId = 0,
	anon_read = 0,
	anon_write = 0,
	repl_failed_response = 0,
	workerpid=ngx.worker.pid()
}


local function gethostname()
	local f = io.popen ("/bin/hostname")
	hostname = f:read("*a") or ""
	f:close()
	hostname =string.gsub(hostname, "\n$", "")
end

local function get_stats_key_prefix()
	if hostname == nil then
		gethostname()
	end
	stats_key_prefix = tostring(ngx.worker.pid()).."_"..hostname
end

function _statscollector.update_local_stats_cache(context)
	--Populate the Request Entry To Be Updated into the Local Cache
	if context.access_key == nil then
		return
	end
	access_key_list[context.access_key.."stats_agl"] = 1
	
	--Update the Local Cache Here
	local access_key = tostring(context.access_key)
	local method = tostring(context.request_method)
	local response = tostring(context.response_code)
	local size = tostring(context.request_length)
	logger:debug("Request Stats as".." A Key as -->"..access_key .."Method -->"..method .." Response -->"..response .."..Size -->"..size)

	local lentry = lcache:exists(access_key.."stats_agl")
	--This is the first the time the cache is being hit with this entry.
	--Possible cases, Worker Process Restart or New User itself
	if lentry == nil then
		local stats_table = {}
		stats_table[method] = 1
		stats_table[response] = 1
		stats_table["size"] = size
		lcache:insert_entry(access_key.."stats_agl", stats_table)
	else
		if lentry[method] == nil then
			lentry[method] = 1
		else 
			lentry[method] = lentry[method] + 1
		end

		if lentry[response] == nil then
			lentry[response] = 1
		else
			lentry[response] = lentry[response] + 1
		end
	        
		--If this is PUT/POST Transaction Record The Size for the same in a different field
		--GET transaction size are recorded in a sepearte field
		if method == "PUT" or method == "POST" then 
			if lentry["put_size"] == nil then
				lentry["put_size"] = size
			else
				lentry["put_size"] = lentry["put_size"] + size
			end
		else 
			if lentry["get_size"] == nil then
				lentry["get_size"] = size
			else
				lentry["get_size"] = lentry["get_size"] + size
			end
		end
	end
	--Comment it in prod
	--lcache:print_cachetable_details(access_key.."stats_agl")
end

function _statscollector.print_req_details(context)
	reqId = tostring(_perReq_info["reqId"])
	startTime = tostring(_perReq_info["startTime"])
        endTime = tostring(_perReq_info["endTime"])
        logger:notice("Request Stats ---> Request Id-->"..reqId.." StartTime-->"..startTime.." EndTime-->"..endTime)
end

function _statscollector.print_stats_details()
	logger:notice("============Stats Details=======================")
        logger:notice("Worker Id : "..tostring(_statscollector_info["workerpid"]))
	logger:notice("Fallback Count: "..tostring(_statscollector_info["fallback_count"]))
	logger:notice("Anon Write: "..tostring(_statscollector_info["anon_write"]))
	logger:notice("Anon Read: "..tostring(_statscollector_info["anon_read"]))
	--[[
	logger:info("Read FallBack Count : "..tostring(_statscollector_info["readfallback_count"]))
	logger:info("Read Request Count : "..tostring(_statscollector_info["readreq_count"]))
	logger:info("Write Request Count : "..tostring(_statscollector_info["writereq_count"]))
	logger:info("Delete Request : "..tostring(_statscollector_info["delreq_count"]))
	logger:info("Set Acl Count : "..tostring(_statscollector_info["setacl_count"]))
	logger:info("Reject Request Count : "..tostring(_statscollector_info["rejectreq_count"]))
	logger:info("Total Request Src Cluster : "..tostring(_statscollector_info["src_cluster_req_count"]))
	logger:info("Total Request Destination Cluster : "..tostring(_statscollector_info["dst_cluster_req_count"]))
	]]--
end

function _statscollector.incr_ctr(key)
	
	--logger:debug("Increment key as "..key)
	local ctrValue = 0
	ctrValue = tonumber(_statscollector_info[key])
	if ctrValue == nil then
		ctrValue = 0
	end
	ctrValue = ctrValue + 1
	--logger:debug("ctrvalue"..tostring(ctrValue))
        _statscollector_info[key] = ctrValue

	--print_stats_details()
end

function _statscollector.update_req_stats(key)
        
	logger:debug("Update Request Stats for "..key)

	if key == "reqId" then
            _statscollector.incr_ctr("globalreqId")
            _perReq_info[key] = _statscollector_info["globalreqId"]
	else 
            _perReq_info[key] = ngx.now()
        end
end

-- Function to Update the Request Statistics for all the Users
-- The Function is called under the time context, every 10 minutes.
-- The Entries are read from the Local Cache, which is updated at every request
-- TBD Need to Time the API to Get the Time it takes to Dump the stats
-- TBD Cache the prefix pid+hostname
function _statscollector.update_stats_remote()
	--The Hostname will be read only once during the first time, post 
	--this the cached value will be read
	if hostname == nil then
		gethostname()
		logger:notice("Hostname is -->"..tostring(hostname))
	end

	if stats_key_prefix == nil then
		get_stats_key_prefix()
	end

	logger:notice("Update Stats to Remote for Hostname -->"..hostname)

	for k,v in pairs(access_key_list) do
		logger:notice("Updating Key for key -->"..k .."value is -->"..v)
		local lentry = lcache:exists(k)
		if lentry ~= nil then
			stats_cache.set(stats_key_prefix.."_"..k, lentry)
		end
	end
	
	local global_stats = {}
	global_stats["fallback_count"] = _statscollector_info["fallback_count"] 
	global_stats["anon_read"] = _statscollector_info["anon_read"] 
	global_stats["anon_write"] = _statscollector_info["anon_write"] 
	global_stats["repl_failed_response"] = _statscollector_info["repl_failed_response"] 
	--Update Generic Stats at the global level
	--Fallback Count Only Implemented Currently
	stats_cache.set(stats_key_prefix.."_".."global_stats_agl", global_stats)

end

-- Function To Update the Bucket Create Statistics
-- Called from the gateway.lua under the bucket create API
function _statscollector.update_bkt_create_stats(context)
	local bucket_create_info = {}

	if hostname == nil then 
		gethostname()
		logger:notice("Hostname is -->"..tostring(hostname))
	end
	
	if stats_key_prefix == nil then
		get_stats_key_prefix()
	end

	if context.access_key == nil or context.bucket_name == nil then
		logger:error("Bucket Name or Access Key Not Specified")
		return
	end

	logger:notice("Updating Bucket Create Stats for Bucket-->"..context.bucket_name)

	bucket_create_info["bucket_name"] = context.bucket_name
	bucket_create_info["access_key"] =  context.access_key
	bucket_create_info["create_tms"] = ngx.time()
	
	--TBD PUT EXception Check Here
	stats_cache.set(stats_key_prefix.."_"..context.bucket_name.."_bucket_create_agl", bucket_create_info)
end


return _statscollector
