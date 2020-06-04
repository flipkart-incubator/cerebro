--Constants which model rate limit entities go here.
--The Rate Limit is modelled across the following classes
--TBD: Update Class
--

--Constants / Enumerations for Different types RATE LIMIT Profiles Identifications
--MODES???
RATE_LIMIT_PROFILE_TYPES = {
	RATE_LIMIT_BAU = 0,
	RATE_LIMIT_DEGRADED = 1,
	RATE_LIMIT_TEMP = 2,  -- Should have expiry Enabled / Implemented
	RATE_LIMIT_SPIKE_EVENTS = 3, -- Mapped to Sale Events / (Pre / Post)
}


--Constants for Timr Quanta
TIME_QUANTA = {
	PER_SECOND = "per_second",
	PER_MINUTE = "per_minute",
	PER_HOUR = "per_hour"
}


--Constants Modeling the Rate Limit Profile Itself.
--Coarse Rate Limiting Profule, which gives RPS base control on the requested resources.
--The Bucket Create and Deletes are seperated out from the Rest of the Object Operations.
--Moreover the Object Operations are, broadly divided into READ, WRITE and DELETE Request Types
--If Further Control of the request is required, a field, indicating, yet another redirection matrix, where
--more finer contorls are specified can be referred to.
RATE_LIMIT_REQUEST_PROFILE_META = {
     RATE_LIMIT_PROFILE_NAME = "rate_limit_request_profile_name",
     -- The type field specifies the template of this rate limit profile. We can have on or many rate limit profiles for a given user.
     -- At any point one of the profiles would be homogenously activated, across all the Nodes. 
     -- The various Profile Types can be, but not limited to as below:-
     --     1. BAU : BAU Specific Profie, which caters to the Daily IOPS
     --     2. DEGRADATION : Profile Applied, while running degraded. This can be either priorotisation of ceration users over, others, or 
     --                      temporarily increase in READ/WRITE IOPS 
     --     3. TIMER / EXPIRY DRIVEN: TBD Description
     --     4. SALE / SPIKE EVENTS: TBD Description

     RATE_LIMIT_PROFILE_TYPE = "rate_limit_profile_type",
     --Bucket Operations GRANT VALUES
     BUCKET_CREATE_GRANT_VALUE = "bucket_create_grant_value",
     BUCKET_CREATE_QUANTA = "bucket_create_quanta",
     BUCKET_DELETE_GRANT_VALUE = "bucket_delete_grant_value",
     BUCKET_DELETE_QUANTA = "bucket_delete_quanta",
     --TBD: Check where to smash the bucket HEAD and LS kind of calls
     --Object Operations GRANT VALUES
     OBJ_READ_GRANT_VALUE = "obj_put_grant_value",
     OBJ_READ_QUANTA = "obj_put_quanta",
     OBJ_WRITE_GRANT_VALUE = "obj_write_grant_value",
     OBJ_WRITE_QUANTA = "obj_write_quanta",
     OBJ_DELETE_GRANT_VALUE = "obj_delete_grant_value",
     OBJ_DELETE_QUANTA = "obj_delete_quanta",
     --Error Codes and Message to be Disptached Once the Limit is Breached
     RATE_LIMIT_REQUEST_BREACH_ERROR_CODE = "rate_limit_request_breach_error_code",
     RATE_LIMIT_REQUEST_BREACH_ERROR_MESSAGE = "rate_limit_request_breach_error_message"
}

--This Following are the MAP for the Bandwidth based Rate Limiting
RATE_LIMIT_BW_PROFILE_META = {
     RATE_LIMIT_BW_PROFILE_NAME = "rate_limit_bw_profile_name",
     -- The type field specifies the template of this rate limit profile. We can have on or many rate limit profiles for a given user.
     -- At any point one of the profiles would be homogenously activated, across all the Nodes. 
     -- The various Profile Types can be, but not limited to as below:-
     --     1. BAU : BAU Specific Profie, which caters to the Daily IOPS
     --     2. DEGRADATION : Profile Applied, while running degraded. This can be either priorotisation of ceration users over, others, or 
     --                      temporarily increase in READ/WRITE IOPS 
     --     3. TIMER / EXPIRY DRIVEN: TBD Description
     --     4. SALE / SPIKE EVENTS: TBD Description

     RATE_LIMIT_PROFILE_TYPE = "rate_limit_profile_type",
     USER_READ_BW_GRANT_VALUE = "user_read_bw_grant_value",
     USER_READ_BW_QUANTA = "user_read_bw_quanta",
     USER_WRITE_BW_GRANT_VALUE = "user_write_bw_grant_value",
     USER_WRITE_BW_QUANTA = "user_write_bw_quanta",
     USER_DELETE_BW_GRANT_VALUE = "user_delete_bw_grant_value",
     USER_DELETE_BW_QUANTA = "user_delete_bw_quanta",
     --Error Codes and Message to be Disptached Once the Limit is Breached
     USER_BW_BREACH_ERROR_CODE = "user_bw_breach_error_code",
     USER_BW_BREACH_ERROR_MESSAGE = "user_bw_breach_error_message"
}


--Constants for the User Metadata
USER_RATE_LIMIT_META = {
	DISPLAY_NAME = "display_name",
	IS_FEDERATED = "is_federated",
	RATE_LIMIT_REQUEST_ENABLED = "rate_limit_request_enabled",
	RATE_LIMIT_BW_ENABLED = "rate_limit_bw_enabled",
	RATE_LIMIT_REQUEST_CURRENT_PROFILE = "rate_limit_request_current_profile",
	RATE_LIMIT_BW_CURRENT_PRFOILE = "rate_limit_bw_current_profile"
}


--Constants for the Response Header Preparation
GRANT_BREACH_RESPONSE = {


}

--RATE LIMIT TUPLE FOR REDIS CELL
RATE_LIMIT_TUPLE_REDIS_CELL = {
	MAX_BURST = "max_burst",
	GRANT_TOKEN = "grant_token",
	TIME  = "time",  --specificy in seconds
	APPLIED_TOKEN = "applied_token", -- for Request Based it is always 1, for BW it is the number of BYTES
}


