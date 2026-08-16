-- ml_trust_score.lua — ML-enhanced trust scoring for PartyVault
-- Learns from historical patterns to predict party trustworthiness

local MLTrustScore = {
    weights = {
        lei_valid = 25,
        lei_checksum = 15,
        country_present = 10,
        country_risk = 20,    -- High-risk jurisdictions penalise
        entity_type = 15,     -- Credit institution vs shell company
        status_active = 10,   -- Active vs suspended
        email_valid = 5,
        historical_penalty = 0,  -- Learned from past violations
    },
    high_risk_countries = {
        KY = true, BS = true, VG = true, BZ = true, PA = true,
        SC = true, CK = true, MU = true, SG_HIGH = true,
    },
    high_risk_entities = {
        SHELL_COMPANY = true,
        OFFSHORE_HOLDING = true,
        PRIVATE_FOUNDATION = true,
    },
    learning_rate = 0.1,
}

-- Calculate ML-enhanced trust score
function MLTrustScore.calculate(party)
    local score = 0
    local max_score = 100
    
    -- LEI validity
    if party.lei_valid == "true" or party.lei_valid == true then
        score = score + MLTrustScore.weights.lei_valid
        if party.lei_status == "OK" then
            score = score + MLTrustScore.weights.lei_checksum
        end
    end
    
    -- Country
    if party.country and #party.country > 0 then
        score = score + MLTrustScore.weights.country_present
        if MLTrustScore.high_risk_countries[party.country] then
            score = score - MLTrustScore.weights.country_risk
        end
    end
    
    -- Entity type
    if MLTrustScore.high_risk_entities[party.entity_type] then
        score = score - MLTrustScore.weights.entity_type
    else
        score = score + MLTrustScore.weights.entity_type * 0.5
    end
    
    -- Status
    if party.status == "ACTIVE" then
        score = score + MLTrustScore.weights.status_active
    elseif party.status == "SUSPENDED" then
        score = score - 30
    end
    
    -- Email
    if party.email_valid == "true" or party.email_valid == true then
        score = score + MLTrustScore.weights.email_valid
    end
    
    -- Clamp to 0-100
    score = math.max(0, math.min(max_score, score))
    
    -- Apply learned penalty (from historical data)
    score = score - MLTrustScore.weights.historical_penalty
    
    return math.max(0, score)
end

-- Learn from a labelled example (adjust weights)
function MLTrustScore.learn(party, actual_trust)
    local predicted = MLTrustScore.calculate(party)
    local error = actual_trust - predicted
    
    -- Simple gradient descent on weights
    MLTrustScore.weights.historical_penalty = 
        MLTrustScore.weights.historical_penalty - MLTrustScore.learning_rate * error
    
    return error
end

-- Batch learn from labelled data
function MLTrustScore.train(examples)
    local total_error = 0
    for _, example in ipairs(examples) do
        total_error = total_error + math.abs(MLTrustScore.learn(example.party, example.actual_trust))
    end
    return total_error / #examples
end

-- Get feature importance
function MLTrustScore.feature_importance()
    return MLTrustScore.weights
end

return MLTrustScore
