-- Held-out ML evaluation (T14)
package.path = "./lua/?.lua;" .. package.path
local MLTrustScore = require("ml_trust_score")

print("=== T14: Held-out ML Evaluation ===\n")

-- Training set
local training = {
    {party = {name="Bank A", lei_valid=true, lei_status="OK", country="DE", entity_type="CREDIT_INSTITUTION", status="ACTIVE", email_valid=true}, actual_trust=95},
    {party = {name="Bank B", lei_valid=true, lei_status="OK", country="FR", entity_type="CREDIT_INSTITUTION", status="ACTIVE", email_valid=true}, actual_trust=92},
    {party = {name="Shell Co", lei_valid=false, country="KY", entity_type="SHELL_COMPANY", status="ACTIVE"}, actual_trust=5},
    {party = {name="Suspended", lei_valid=false, country="BS", entity_type="EXCHANGE", status="SUSPENDED"}, actual_trust=3},
}

-- Held-out set (never seen during training)
local held_out = {
    {party = {name="Bank C", lei_valid=true, lei_status="OK", country="GB", entity_type="CREDIT_INSTITUTION", status="ACTIVE", email_valid=true}, actual_trust=90},
    {party = {name="Another Shell", lei_valid=false, country="VG", entity_type="SHELL_COMPANY", status="ACTIVE"}, actual_trust=8},
}

-- Train on training set
MLTrustScore.train(training)

-- Evaluate on held-out set
local total_error = 0
for _, example in ipairs(held_out) do
    local predicted = MLTrustScore.calculate(example.party)
    local error = math.abs(predicted - example.actual_trust)
    total_error = total_error + error
    print(string.format("  %-20s: predicted=%3d  actual=%3d  error=%.1f",
          example.party.name, predicted, example.actual_trust, error))
end

local avg_error = total_error / #held_out
print(string.format("\nHeld-out average error: %.2f", avg_error))
print()

if avg_error < 20 then
    print("=== T14 PASS: held-out error below 20 points ===\n")
else
    print("=== T14 FAIL: held-out error too high ===\n")
end
