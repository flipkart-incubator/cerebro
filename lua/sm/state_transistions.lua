--[[ The modules captures the various valid 
--  state transistion for the Account And Buckets.
--  This is a simple mapping as follows
--  
--  [Current State] : [Next Valid State]
--  Example :- 
--  [ACCOUNT_MIGRATION_DORMANT] = [ACCOUNT_MIGRATION_IN_PROGRESS]
--  
--  Tables are maintained for each of the context (SM)
]]
local logger = require(".lua.common.logger").new("state_transistions.lua")
local constants = require(".lua.common.constants")
local _smctx = {}

local account_migration_sm = {
	[tonumber(constants.ACCOUNT_MIGRATION_DORMANT)]  = constants.ACCOUNT_MIGRATION_IN_PROGRESS,
	[tonumber(constants.ACCOUNT_MIGRATION_IN_PROGRESS)]  = {constants.ACCOUNT_MIGRATION_STOPPED, constants.ACCOUNT_MIGRATION_COMPLETED},
	[tonumber(constants.ACCOUNT_MIGRATION_STOPPED)] = constants.ACCOUNT_MIGRATION_IN_PROGRESS,
	[tonumber(constants.ACCOUNT_MIGRATION_COMPLETED)] = "NIL",
	[tonumber(constants.ACCOUNT_NEW_ONBOARDING)] = "NIL"
}

local bucket_migration_sm = {
	[tonumber(constants.BUCKET_MIGRATION_DORMANT)]  = constants.BUCKET_MIGRATION_IN_PROGRESS,
	[tonumber(constants.BUCKET_MIGRATION_IN_PROGRESS)]  = {constants.BUCKET_MIGRATION_STOPPED, constants.BUCKET_MIGRATION_COMPLETED},
	[tonumber(constants.BUCKET_MIGRATION_STOPPED)] = constants.BUCKET_MIGRATION_IN_PROGRESS,
	[tonumber(constants.BUCKET_MIGRATION_COMPLETED)] = "NIL",
	[tonumber(constants.BUCKET_NEW_CREATE)] = "NIL"
}

local function getNextTrans_Account(currState)
	return account_migration_sm[tonumber(currState)]
end

local function getNextTrans_Bucket(currState)
	return bucket_migration_sm[tonumber(currState)]
end


function _smctx.isValidStateTransition_Account(currState, nextState)

	local isValidTrans = false
        
	if nextState == currState then 
            -- Self Transition
	    logger:debug("Self Transistion")
	    return true
	end

	--We need special Handling for New Account Create
	--Since this is a transiiton from (*) -> ACCOUNT_NEW_ONBOARDING
	if currState == constants.ACCOUNT_MIGRATION_DORMANT and nextState == constants.ACCOUNT_NEW_ONBOARDING then
		
		return true
        end

	logger:debug(string.format("Account State Transistions currState (%s) , nextState(%s)", tostring(currState), tostring(nextState)))

	local nextPState = getNextTrans_Account(currState)

	if currState == constants.ACCOUNT_MIGRATION_IN_PROGRESS then 
		logger:debug("Next Possible States are"..tostring(nextPState[1]).." "..tostring(nextPState[2]))
	   	if nextState == nextPState[1] or nextState == nextPState[2] then
		       	isValidTrans = true
		end
	else 
		logger:debug("Next Possible States are"..tostring(nextPState))
	        if (nextState) == (nextPState) then
		        isValidTrans = true
		end
	end
	
	return isValidTrans
end

function _smctx.isValidStateTransition_Bucket(currState, nextState)

	local isValidTrans = false
        
	if nextState == currState then 
            -- Self Transition
	    logger:debug("Self Transistion")
	    return true
	end

	--We need special Handling for New Bucket Create
	--Since this is a transiiton from (*) -> BUCKET_NEW_CREATE
	if currState == constants.BUCKET_MIGRATION_DORMANT and nextState == constants.BUCKET_NEW_CREATE then
		return true
        end

	logger:debug(string.format("Bucket State Transistions currState (%s) , nextState(%s)", tostring(currState), tostring(nextState)))

	local nextPState = getNextTrans_Bucket(currState)

	if currState == constants.BUCKET_MIGRATION_IN_PROGRESS then 
		logger:debug("Next Possible States are"..tostring(nextPState[1]).." "..tostring(nextPState[2]))
	   	if nextState == nextPState[1] or nextState == nextPState[2] then
		       	isValidTrans = true
		end
	else 
		logger:debug("Next Possible States are"..tostring(nextPState))
	        if (nextState) == (nextPState) then
		        isValidTrans = true
		end
	end
	
	return isValidTrans
end


--API to Test Valid State Transistions.
function _smctx.test()
    logger:debug("Testing State Transisitons for Accounts")
    logger:debug("Account Dormant ->"..account_migration_sm[tonumber(constants.ACCOUNT_MIGRATION_DORMANT)])
    logger:debug("Account Migration in Progress->"..account_migration_sm[tonumber(constants.ACCOUNT_MIGRATION_IN_PROGRESS)][1])
    logger:debug("Account Migration in Progress->"..account_migration_sm[tonumber(constants.ACCOUNT_MIGRATION_IN_PROGRESS)][2])
    logger:debug("Account Migration Stopped->"..account_migration_sm[tonumber(constants.ACCOUNT_MIGRATION_STOPPED)])
    logger:debug("Account Migration Completed->"..account_migration_sm[tonumber(constants.ACCOUNT_MIGRATION_COMPLETED)])
    logger:debug("Account New Onboarding->"..account_migration_sm[tonumber(constants.ACCOUNT_NEW_ONBOARDING)])
    
    logger:debug("Testing State Transisitons for Buckets")
    logger:debug("Bucket Dormant ->"..bucket_migration_sm[tonumber(constants.BUCKET_MIGRATION_DORMANT)])
    logger:debug("Bucket Migration in Progress->"..bucket_migration_sm[tonumber(constants.BUCKET_MIGRATION_IN_PROGRESS)][1])
    logger:debug("Bucket Migration in Progress->"..bucket_migration_sm[tonumber(constants.BUCKET_MIGRATION_IN_PROGRESS)][2])
    logger:debug("Bukcet Migration Stopped->"..bucket_migration_sm[tonumber(constants.BUCKET_MIGRATION_STOPPED)])
    logger:debug("Bucket Migration Completed->"..bucket_migration_sm[tonumber(constants.BUCKET_MIGRATION_COMPLETED)])
    logger:debug("Bucket New Onboarding->"..bucket_migration_sm[tonumber(constants.BUCKET_NEW_CREATE)])
end

return _smctx




