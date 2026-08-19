-- PostgreSQL backend option (v8.0)
-- Multi-node deployment support

local PostgresBackend = {
    enabled = false,
    connection_string = "postgresql://localhost/partyvault",
}

function PostgresBackend.enable(connection)
    PostgresBackend.enabled = true
    PostgresBackend.connection_string = connection or PostgresBackend.connection_string
    print("PostgreSQL backend enabled")
end

function PostgresBackend.migrate()
    -- In production: run SQL migrations
    return {status = "MIGRATED", schema = "v8.0"}
end

return PostgresBackend
