#!/usr/bin/env luajit
-- ============================================================
-- PartyVault Business Rule Engine (LuaJIT)
--
-- Hot-swappable regulatory classification rules.
-- In production, rules would be loaded from external config
-- and updated without recompilation — this is the key
-- architectural advantage of embedded Lua.
-- ============================================================

local ffi = require("ffi")

io.stderr:write("[LUAJIT/RULES] PartyVault Business Rule Engine starting...\n")

-- ============================================================
-- REGULATORY CLASSIFICATION RULES
-- These would be loaded from external files in production,
-- enabling regulatory teams to update rules without code changes.
-- ============================================================

local rules = {}

-- Rule: KYC Level Assignment
rules.kyc_level = function(party)
    -- CSD/ICSD entities get simplified due diligence
    if party.entity_type == "CSD" then
        return "SIMPLIFIED", "Regulated FMI — simplified due diligence per MiFID II Art. 13"
    end
    
    -- Credit institutions in EU/EEA — standard due diligence
        if party.entity_type == "CREDIT_INSTITUTION" and (party.country_valid == "1" or party.country_valid == "true") then
        local eu_countries = {
            DE=true, FR=true, BE=true, NL=true, LU=true, IT=true, ES=true,
            PT=true, AT=true, FI=true, IE=true, GR=true, SE=true, DK=true,
            PL=true, CZ=true, HU=true, RO=true, BG=true, HR=true, SI=true,
            SK=true, LT=true, LV=true, EE=true, CY=true, MT=true
        }
        if eu_countries[party.country] then
            return "STANDARD", "EU credit institution — standard CDD"
        end
    end
    
    -- Offshore jurisdictions — enhanced due diligence
    local offshore = { KY=true, BS=true, BM=true, VG=true, JE=true, GG=true, IM=true, PA=true }
    if offshore[party.country] then
        return "ENHANCED", "Offshore jurisdiction — enhanced due diligence required"
    end
    
    -- Shell companies always enhanced
    if party.entity_type == "SHELL_COMPANY" then
        return "ENHANCED", "Shell company classification — enhanced due diligence mandatory"
    end
    
    -- Default
    return "STANDARD", "Default classification"
end

-- Rule: Regulatory Reporting Obligations
rules.reporting_obligations = function(party)
    local obligations = {}
    
    -- EMIR reporting for certain entity types
    if party.entity_type == "CREDIT_INSTITUTION" or 
       party.entity_type == "INVESTMENT_FIRM" then
        table.insert(obligations, "EMIR_TR_REPORTING")
    end
    
    -- SFTR for securities financing
    if party.entity_type == "CSD" then
        table.insert(obligations, "SFTR_REPORTING")
        table.insert(obligations, "CSDR_SETTLEMENT_DISCIPLINE")
    end
    
    -- LEI mandatory for all regulated entities
        if party.lei_valid ~= "1" and party.lei_valid ~= "true" then
        table.insert(obligations, "LEI_REQUIRED_MISSING")
    end
    
    -- FATCA/CRS for non-EU
    local eu = {
        DE=true, FR=true, BE=true, NL=true, LU=true, IT=true, ES=true,
        PT=true, AT=true, FI=true, IE=true, GR=true
    }
    if not eu[party.country] then
        table.insert(obligations, "CRS_REPORTING")
    end
    
    return obligations
end

-- Rule: Risk Scoring
rules.risk_score = function(party)
    local score = 0
    local factors = {}
    
    -- LEI validation
        if party.lei_valid ~= "1" and party.lei_valid ~= "true" then
        score = score + 30
        table.insert(factors, "NO_VALID_LEI(+30)")
    end
    
    -- Country risk
    local high_risk_countries = { KY=40, BS=40, BM=35, VG=45, PA=50 }
    local cr = high_risk_countries[party.country]
    if cr then
        score = score + cr
        table.insert(factors, "HIGH_RISK_COUNTRY(+" .. cr .. ")")
    end
    
    -- Entity type risk
    if party.entity_type == "SHELL_COMPANY" then
        score = score + 50
        table.insert(factors, "SHELL_COMPANY(+50)")
    end
    
    -- Status risk
    if party.status == "SUSPENDED" then
        score = score + 40
        table.insert(factors, "SUSPENDED_ENTITY(+40)")
    end
    
    -- Missing data risk
    if party.trust_score and tonumber(party.trust_score) then
        local trust = tonumber(party.trust_score)
        if trust < 50 then
            local penalty = math.floor((50 - trust) / 2)
            score = score + penalty
            table.insert(factors, "LOW_TRUST_SCORE(+" .. penalty .. ")")
        end
    end
    
    -- Classify
    local classification
    if score >= 80 then classification = "CRITICAL"
    elseif score >= 50 then classification = "HIGH"
    elseif score >= 25 then classification = "MEDIUM"
    else classification = "LOW"
    end
    
    return score, classification, factors
end

-- Rule: Sanctions Screening Flags (simplified)
rules.sanctions_flags = function(party)
    local flags = {}
    
    -- Placeholder: In production, this would check against
    -- EU consolidated sanctions list, OFAC SDN, UN lists, etc.
    
    -- Demo: flag suspended entities
    if party.status == "SUSPENDED" then
        table.insert(flags, "REVIEW:SUSPENDED_STATUS")
    end
    
    -- Demo: flag entities with known problematic patterns
    if party.legal_name and party.legal_name:match("[Oo]ffshore") then
        table.insert(flags, "REVIEW:NAME_CONTAINS_OFFSHORE")
    end
    
    if party.entity_type == "EXCHANGE" and party.status == "SUSPENDED" then
        table.insert(flags, "ALERT:SUSPENDED_EXCHANGE")
    end
    
    return flags
end

-- ============================================================
-- PROCESSING ENGINE
-- ============================================================

local function parse_identity_line(line)
    -- Parse Zig crypto output format:
    -- IDENTITY|id|name|country|fingerprint|lei|entity_type|lei_valid=X|country_valid=X|trust=Y
    local parts = {}
    for part in line:gmatch("[^|]+") do
        table.insert(parts, part)
    end
    
    if #parts < 9 or parts[1] ~= "IDENTITY" then
        return nil
    end
    
    local party = {
        party_id    = parts[2],
        legal_name  = parts[3],
        country     = parts[4],
        fingerprint = parts[5],
        lei         = parts[6],
        entity_type = parts[7],
    }
    
    -- Parse key=value fields
    for i = 8, #parts do
        local key, val = parts[i]:match("^(%w+)=(.+)$")
        if key then
            party[key] = val
        end
    end
    
    -- Parse trust score
    party.trust_score = party.trust or "0"
    -- Parse status from notes or default
    party.status = party.status or "ACTIVE"
    
    return party
end

-- Read from stdin (piped from Zig output or cleansed data)
local processed = 0
local high_risk_count = 0

-- Output header
print("PARTY_ID|LEGAL_NAME|COUNTRY|FINGERPRINT|KYC_LEVEL|KYC_REASON|RISK_SCORE|RISK_CLASS|RISK_FACTORS|OBLIGATIONS|SANCTIONS_FLAGS")

for line in io.stdin:lines() do
    -- Skip comments and headers
    if line:match("^#") or line:match("^PARTY_ID") or line:match("^SKIP") then
        goto continue
    end
    
    local party = parse_identity_line(line)
    if not party then
        goto continue
    end
    
    -- Apply all rules
    local kyc_level, kyc_reason = rules.kyc_level(party)
    local risk_score, risk_class, risk_factors = rules.risk_score(party)
    local obligations = rules.reporting_obligations(party)
    local sanctions = rules.sanctions_flags(party)
    
    -- Format output
    local risk_factors_str = table.concat(risk_factors, ";") 
    if risk_factors_str == "" then risk_factors_str = "NONE" end
    
    local obligations_str = table.concat(obligations, ";")
    if obligations_str == "" then obligations_str = "NONE" end
    
    local sanctions_str = table.concat(sanctions, ";")
    if sanctions_str == "" then sanctions_str = "CLEAR" end
    
    print(string.format("%s|%s|%s|%s|%s|%s|%d|%s|%s|%s|%s",
        party.party_id or "?",
        party.legal_name or "?",
        party.country or "?",
        (party.fingerprint or ""):sub(1, 16) .. "...",
        kyc_level,
        kyc_reason,
        risk_score,
        risk_class,
        risk_factors_str,
        obligations_str,
        sanctions_str
    ))
    
    processed = processed + 1
    if risk_class == "HIGH" or risk_class == "CRITICAL" then
        high_risk_count = high_risk_count + 1
    end
    
    ::continue::
end

io.stderr:write(string.format(
    "[LUAJIT/RULES] Processed %d parties | %d flagged HIGH/CRITICAL risk\n",
    processed, high_risk_count
))
io.stderr:write("[LUAJIT/RULES] Rule engine complete.\n")
