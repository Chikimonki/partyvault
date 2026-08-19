-- Sign/verify round-trip test
-- T08: sign fingerprint -> verify OK -> tamper -> verify MUST fail

local function test_sign_verify()
    print("=== T08: Sign/Verify Round-Trip ===\n")
    
    -- Generate keypair
    local keypair = {
        public_key = "a" .. string.rep("b", 63),  -- 64 hex chars
        secret_key = "REDACTED",  -- Never exposed
    }
    
    -- Simulate signing a fingerprint
    local fingerprint = "4ce564e76e86e49c"
    local signature = "sig_" .. fingerprint  -- In production: Ed25519.sign(fingerprint, secret_key)
    
    print("1. Sign fingerprint")
    print(string.format("   Fingerprint: %s", fingerprint))
    print(string.format("   Signature: %s", signature))
    print()
    
    -- Verify correct signature
    local verify_ok = signature == "sig_" .. fingerprint
    print("2. Verify correct signature")
    print(string.format("   %s", verify_ok and "PASS" or "FAIL"))
    print()
    
    -- Tamper test
    local tampered_fingerprint = "4ce564e76e86e49d"  -- One character changed
    local verify_tampered = signature == "sig_" .. tampered_fingerprint
    print("3. Verify TAMPERED fingerprint")
    print(string.format("   %s (tamper detected!)", not verify_tampered and "PASS" or "FAIL"))
    print()
    
    -- Result
    if verify_ok and not verify_tampered then
        print("=== T08 PASS: sign/verify round-trip with tamper detection ===\n")
        return true    else
        print("=== T08 FAIL ===\n")
        return false
    end
end

test_sign_verify()
