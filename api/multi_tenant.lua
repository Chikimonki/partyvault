-- Multi-tenant production deployment (v9.0)
-- Tenant isolation for multiple financial institutions

local MultiTenant = {
    tenants = {},
}

function MultiTenant.register_tenant(name, tenant_id)
    MultiTenant.tenants[tenant_id] = {
        name = name,
        parties = {},
        api_keys = {},
        rate_limits = {},
    }
    return tenant_id
end

function MultiTenant.isolate(tenant_id, party_data)
    -- Ensure party data is isolated to the tenant
    if not MultiTenant.tenants[tenant_id] then
        return {error = "Unknown tenant"}
    end
    
    MultiTenant.tenants[tenant_id].parties[party_data.id] = party_data
    return {status = "ISOLATED", tenant = tenant_id}
end

function MultiTenant.list_tenants()
    local result = {}
    for tenant_id, tenant in pairs(MultiTenant.tenants) do
        table.insert(result, {
            tenant_id = tenant_id,
            name = tenant.name,
            party_count = #tenant.parties,
        })
    end
    return result
end

return MultiTenant
