-- SQLite persistent storage (v7.0)
-- Party records stored for multi-tenant support

local Storage = {
    db_path = "./output/partyvault.db",
    parties = {},
}

function Storage.init()
    -- In production: CREATE TABLE parties (id TEXT PRIMARY KEY, name TEXT, lei TEXT, trust_score REAL)
    print("SQLite storage initialised (in-memory for demo)")
end

function Storage.save_party(party_id, party_data)
    Storage.parties[party_id] = party_data
    return true
end

function Storage.get_party(party_id)
    return Storage.parties[party_id]
end

function Storage.list_parties()
    return Storage.parties
end

function Storage.delete_party(party_id)
    Storage.parties[party_id] = nil
    return true
end

return Storage
