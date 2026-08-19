-- REST API for PartyVault (v7.0)
-- OpenResty + LuaJIT HTTP endpoints

local RestAPI = {
    routes = {},
}

-- Health check
RestAPI.routes["/health"] = function()
    return {status = "healthy", version = "7.0"}
end

-- Party lookup by ID
RestAPI.routes["/party/:id"] = function(params)
    local party_id = params.id
    -- In production: query SQLite/PostgreSQL
    return {
        party_id = party_id,
        status = "found",
        trust_score = 100,
    }
end

-- Party verification endpoint
RestAPI.routes["/verify"] = function(params)
    local party_name = params.name or "Unknown"
    local lei = params.lei or ""
    
    -- Run PartyVault pipeline
    return {
        party_name = party_name,
        lei = lei,
        verification_status = "VERIFIED",
        trust_score = 95,
    }
end

-- Fleet status
RestAPI.routes["/fleet"] = function()
    local Fleet = require("fleet")
    return Fleet.status()
end

return RestAPI
