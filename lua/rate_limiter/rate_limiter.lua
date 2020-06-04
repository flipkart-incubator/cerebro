--The Interface Called by the Requests to Check for a Particular Resource, limit and its validity

local logger = require(".lua.common.logger").new("redis-rate-limiter")
local constants = require(".lua.rate_limiter.common.constants")

-- A Rate Limit Key is Framed as Follows:-
-- For Normal Request:-
--     rate_limit_key = access_key + cluster_name + rate_agl
-- If the request is anonymous:-
--     Lookup local cache for the bucket to access_key mapping
--     rate_limit_key = access_key + cluster_name_ rate_agl
--     TBD : Relook Implementation of Anon lookup
local function get_rate_limit_key(cluster_name, context)
      local key = nil
      local cluster_name = cluster_name
      local access_key = context.access_key
      
      --The Rate Limit Key is constructed by smashing the user access_key 
      --and the cluster name
      --In case of Anonymous Request, the anonymous rate limit of the particular
      --cluster is applied.
      --TBD : Enhance the Anonymous at per User level
      if access_key == nil then
          key = "anon"..cluster_name
      else
          key = access_key..cluster_name
      end

      --TBD: Possible Optimization of Prefetching and Storing the Key
      return key
end

--The API is Responsible for Looking up the Configured Values, for a particular Config
--This Limit is Used to Call the Rate Limiter Layer
local function get_rate_meta(key)

	local max_burst = nil
        local grant_token = nil
	local time = nil
	local applied_token = nil
       
	max_burst, grant_token, time, applied_token = redis_handle.get_configured_token(key)

end

local function check_limit(key, type)
	
	local max_burst = nil
        local grant_token = nil
	local time = nil
	local applied_token = nil
	local response = nil
    
	if type == constants.BANDWIDTH  then
	     logger:notice("Bandwith Not Supported")	     
	else 
		max_burst, grant_token, time, applied_token = get_rate_meta(key..constants.REQUEST)
		response = redis_handle.check_grant(key..constants.REQUEST, max_burst, grant_token, time, applied_token)
	end

	return response
end

--The Entry Point of the Function
--Takes as input the cluster name and the context 
--
--  Output: 4 tuple <Grant Result, Error Code, Message, Headers (optional) > 
--     Success: Rate Limit is Granted (0, nil, nil, nil)
--     Failure: Rate Limit is Breached(1, 429, SlowDwon, [ ])
--     Failure: Rate Limit is Breached(1, 503, SlowDown, [X retry After : ,] [ X_Limit Lect : ])
--
local function check_rate_limit(cluster_name, context)

        local user_rate_request_limit_enabled = nil
	local user_nw_limit_enabled = nil
        local bw_grant = nil
	local request_grant = nil
	local rate_limit_key = nil

	rate_limit_key = get_rate_limit_key(cluster_name, context)

	if (rate_limit_key == nil) then 
		logger:error("Nil Key")
		return -1
        end


        result,tuple = rate_limiter_handle.get_rate_meta(access_key)
	--TBD: Translation Logic Below this layer
	--
	if result ~= NOT_AVAIALBLE then 
	    logger:error("Rate Limiting Not Enabled, passing through")
	    --Emit a Metric, or Rely on Log Parsing to Catch the Same
	    return
        end	
        
        
        --Parse the Result Tuple Here
	if user_nw_limit_enabled == 1 then 
            bw_grant = rate_limit_handler.check_limit(rate_limit_key, constants.BANDWIDTH)
	
	end


	--Parse the Result Tuple Here
	if user_rate_request_enabled == 1 then 
            request_grant = rate_limit_handler.check_limit(rate_limit_key, constants.REQUEST)
	    if request_grant == 0 then 
		    --TESTING: EMIT A Metric Somwhere in this Layer
		    return 1, nil, nil, nil
            else 
		return 0, 503, "SlowDown", nil
           end
	end
        
	--If the user Breached the Bandwidth
	if bw_grant == BRECAHED then 
		send_response(akey, context)
	end

        --If the user Breached the RPS
	if request_grant == BREACHED then 
		send_response(akey, context)
	end
        
	return
end

--The Entry Point of the Function
--Takes as input the cluster name and the context 
--

--The Entry Point of the Function
--Takes as input the cluster name and the context 
--
--  Output: 4 tuple <Grant Result, Error Code, Message, Headers (optional) > 
--     Success: Rate Limit is Granted (0, nil, nil, nil)
--     Failure: Rate Limit is Breached(1, 429, SlowDwon, [ ])
--     Failure: Rate Limit is Breached(1, 503, SlowDown, [X retry After : ,] [ X_Limit Lect : ])
--
local function check_rate_limit(context)
    local res = nil
    local status = false
    local rate_limiter_handle = require(".lua.service.redis_connection_manager").get("redis-cluster-prod-test")

    local key = get_rate_limit_key(clusterName, context)


    res, status = rate_limiter_handle:clthrottle("testkey123", {15,30,60,1})
    logger:notice("Res is ... --> "..tostring(res[1]).." "..tostring(res[2]).." "..tostring(res[3]).." "..tostring(res[4].." "..tostring(res[5])))
    logger:notice("Status is ... --> "..tostring(status))
    return 
end

return {
	check_rate_limit = check_rate_limit
}

