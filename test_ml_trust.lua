package.path = "./lua/?.lua;" .. package.path

local MLTrustScore = require("ml_trust_score")

print("=== ML Trust Score Test ===\n")

-- Test parties
local parties = {
    {name = "Deutsche Bank", lei_valid = true, lei_status = "OK", country = "DE", entity_type = "CREDIT_INSTITUTION", status = "ACTIVE", email_valid = true},
    {name = "Shady Offshore", lei_valid = false, country = "KY", entity_type = "SHELL_COMPANY", status = "ACTIVE"},
    {name = "FTX Trading", lei_valid = false, country = "BS", entity_type = "EXCHANGE", status = "SUSPENDED"},
    {name = "Goldman Sachs", lei_valid = true, lei_status = "OK", country = "GB", entity_type = "CREDIT_INSTITUTION", status = "ACTIVE", email_valid = true},
}

print("Rule-based vs ML Trust Scores:\n")
for _, party in ipairs(parties) do
    local ml_score = MLTrustScore.calculate(party)
    print(string.format("  %-20s: ML trust = %.0f", party.name, ml_score))
end

print()

-- Demonstrate learning
print("Training with labelled examples...")
local examples = {
    {party = parties[1], actual_trust = 95},  -- Deutsche Bank should be high
    {party = parties[2], actual_trust = 10},  -- Shady Offshore should be low
    {party = parties[3], actual_trust = 5},   -- FTX should be very low
}

local avg_error = MLTrustScore.train(examples)
print(string.format("  Average training error: %.2f", avg_error))
print()

-- Re-test after learning
print("After learning:")
for _, party in ipairs(parties) do
    local ml_score = MLTrustScore.calculate(party)
    print(string.format("  %-20s: ML trust = %.0f", party.name, ml_score))
end

print()
print("=== ML Trust Score Working ===")
