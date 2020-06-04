local constants = require(".lua.common.constants")
local logger = require(".lua.common.logger").new("request_builder.lua")
local util = require(".lua.common.util")
local cluster_status_cache = require(".lua.cache.cluster_status_cache")
local cmgr = require(".lua.config_manager.cfgmgr")

-- Returns request method
-- Returns: string - Request's method
local function get_request_method()
	return ngx.req.get_method()
end

-- Returns request headers
-- Returns: Lua Table - Headers in table (hashtable) format
local function get_request_headers()
	return ngx.req.get_headers()
end

-- Returns request payload
-- Returns: string - request payload 
local function get_request_body()
	return util.get_request_body()
end

-- Returns request uri
-- Returns: string - uri
local function get_request_uri()
	return ngx.var.request_uri
end

-- Returns request location
-- Returns: string - location
local function get_request_location(cluster_name, access_key, request_method)
 
	local target_read_ip = nil
	local target_write_ip = nil
	local cluster_endpoint = {}
	cluster_endpoint = cluster_status_cache.getField(cluster_name,
			"cluster_read_endpoint","cluster_write_endpoint")
	target_read_ip = cluster_endpoint[1]
	target_write_ip = cluster_endpoint[2]
	--[[
	target_read_ip = cluster_status_cache.get_read_endpoint(cluster_name)
	target_write_ip = cluster_status_cache.get_write_endpoint(cluster_name)
	--target_read_ip,target_write_ip = cmgr.get_cluster_detail(cluster_name)
	]]--
        logger:info("Cluster Name -> "..cluster_name.."Read Ip ->"..target_read_ip .."Write Ip ->"..target_write_ip)
	
	if target_read_ip == nil or target_write_ip == nil then
		logger:error("Either Read Or Write Target IP is Nill")
		util.send_response(500)
		return
	end
        
        local target_ip = nil

	if util.is_read_request(request_method) then
             target_ip = target_read_ip 
	else 
             target_ip = target_write_ip
	end

	--Set Downstream ELB IP HERE
	ngx.var.dselb = target_ip
	--Set URI Here, This Will be passed Later to the Subrequest
	ngx.var.dsuri = ngx.var.request_uri 
        
	logger:notice("V2 Redirect SR Target IP -->"..ngx.var.dselb .."SR Target URI -->"..ngx.var.dsuri)
	return "/" .. "UPSTREAM_REDIRECT"

end

-- Builds Request object
-- Param: cluster_name - Cluster name for which request has to be sent
-- Returns: Request object
local function build_request(cluster_name, access_key, request_method, auth_v4)
	local req = {}
	
	req.method = get_request_method()
	req.uri = get_request_uri()
	req.location = get_request_location(cluster_name, access_key, request_method, auth_v4)
	req.headers = get_request_headers()
	req.body = get_request_body()
        --[[
	req.uri = ngx.var.args and ngx.var.uri .. "?" .. ngx.var.args or ngx.var.uri
	req.method = ngx.req.get_method()
	req.headers = ngx.req.get_headers()
	req.body = ngx.req.get_body_data()
	req.location = get_request_location(cluster_name, access_key)
	-]]
         --Printing Body Can Cause Debug File To Bloat	
	--logger:info(string.format("Method: %s\nUri: %s\nLocation: %s\nHeaders: %s\nBody: %s\n", req.method, req.uri, req.location, util.stringify_dict(req.headers), tostring(req.body)))
        
	logger:info(string.format("Method: %s\nUri: %s\nLocation: %s\nHeaders: %s\n", req.method, req.uri, req.location, util.stringify_dict(req.headers)))

	return req
end

return {
	build_request = build_request,
}
