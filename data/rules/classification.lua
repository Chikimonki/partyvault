-- ============================================================
-- PartyVault Classification Rules (Hot-Swappable Config)
-- 
-- In a production system, this file would be loaded at runtime
-- and could be updated by compliance teams without redeployment.
-- This is the architectural advantage of LuaJIT as a rule engine:
-- business logic as data, not compiled code.
-- ============================================================

return {
    version = "1.0.0",
    effective_date = "2025-03-24",
    
    -- Jurisdiction risk tiers
    jurisdiction_tiers = {
        TIER_1 = { "DE", "FR", "BE", "NL", "LU", "FI", "AT", "IE" },
        TIER_2 = { "GB", "CH", "US", "CA", "JP", "AU", "SG", "HK" },
        TIER_3 = { "KY", "BS", "BM", "VG", "JE", "GG", "IM", "PA" },
    },
    
    -- Entity classification thresholds
    thresholds = {
        high_value_entity_eur = 1000000000,   -- 1B EUR
        suspicious_revenue_min = 0,
        suspicious_revenue_max = 1000,
    },
    
    -- Regulatory frameworks by entity type
    regulatory_frameworks = {
        CREDIT_INSTITUTION = { "CRD_V", "MiFID_II", "EMIR", "AMLD_VI" },
        INVESTMENT_FIRM    = { "MiFID_II", "EMIR", "AMLD_VI" },
        CSD                = { "CSDR", "EMIR", "SFTR" },
        EXCHANGE           = { "MiFID_II", "MAR", "EMIR" },
        SHELL_COMPANY      = { "AMLD_VI", "DAC_6" },
    },
}
