package.path = "./lua/?.lua;" .. package.path
local MLTrustScore = require("ml_trust_score")

print("=== ML TRUST SCORE DEEP DIVE ===\n")

-- Test every party type
local parties = {
    {name = "Deutsche Bank", lei_valid = true, lei_status = "OK", country = "DE", entity_t>    {name = "BNP Paribas", lei_valid = true, lei_status = "OK", country = "FR", entity_typ>    {name = "Goldman Sachs", lei_valid = true, lei_status = "OK", country = "GB", entity_t>    {name = "Acme Capital", lei_valid = false, country = "", entity_type = "INVESTMENT_FIR>    {name = "Shady Offshore", lei_valid = false, country = "KY", entity_type = "SHELL_COMP>    {name = "FTX Trading", lei_valid = false, country = "BS", entity_type = "EXCHANGE", st>    {name = "Clearstream", lei_valid = true, lei_status = "CHECKSUM_FAIL", country = "LU",>}

print("Trust scores before training:\n")
for _, party in ipairs(parties) do
    local score = MLTrustScore.calculate(party)
    print(string.format("  %-25s: %3d", party.name, score))
end

-- Train with labelled examples
print("\nTraining with 5 labelled examples...\n")
local examples = {
    {party = parties[1], actual_trust = 95},  -- Deutsche Bank: excellent
    {party = parties[2], actual_trust = 92},  -- BNP Paribas: excellent
    {party = parties[3], actual_trust = 90},  -- Goldman Sachs: excellent
    {party = parties[5], actual_trust = 5},   -- Shady Offshore: terrible
    {party = parties[6], actual_trust = 3},   -- FTX: catastrophic
}

local total_error = MLTrustScore.train(examples)
print(string.format("Average training error: %.2f\n", total_error))

print("Trust scores after training:\n")
for _, party in ipairs(parties) do
    local score = MLTrustScore.calculate(party)
    print(string.format("  %-25s: %3d", party.name, score))
end

-- Feature importance
print("\nFeature weights (what the ML considers important):")
for feature, weight in pairs(MLTrustScore.feature_importance()) do
    print(string.format("  %-30s: %d", feature, weight))
end
