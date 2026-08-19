-- EU Consolidated Sanctions List screening (v7.0)
-- Local screening for air-gapped mode

local SanctionsScreening = {
    sanctions_list = {},
    enabled = false,
}

function SanctionsScreening.enable()
    SanctionsScreening.enabled = true
    print("Sanctions screening enabled")
end

function SanctionsScreening.add_to_list(entity_name, reason)
    SanctionsScreening.sanctions_list[entity_name:lower()] = {
        name = entity_name,
        reason = reason,
        added_at = os.time(),
    }
end

function SanctionsScreening.check(party_name)
    if not SanctionsScreening.enabled then
        return {status = "OFFLINE", note = "Air-gapped mode"}
    end
    
    local match = SanctionsScreening.sanctions_list[party_name:lower()]
    if match then
        return {
            status = "SANCTIONED",
            reason = match.reason,
            risk = "CRITICAL",
        }
    end
    
    return {status = "CLEAR", risk = "LOW"}
end

-- Pre-populate with known sanctioned entities (example)
SanctionsScreening.add_to_list("FTX Trading Ltd", "Insolvency and fraud")
SanctionsScreening.add_to_list("Shady Offshore Holdings", "Shell company risk")

return SanctionsScreening
