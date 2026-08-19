package.path = "./lua/?.lua;" .. package.path
local Fleet = require("fleet")

print("=== FLEET DEEP DIVE ===\n")

-- Register financial institutions
local institutions = {
    {name = "Clearstream", lei = "5299000J2N45DDNE4Y28"},
    {name = "Euroclear", lei = "549300OZ46M7KZ8I6D96"},
    {name = "BNY Mellon", lei = "549300HY8H0Z7V7E8H44"},
    {name = "Deutsche Bank", lei = "7LTWFZYICNSX8D621K86"},
    {name = "Goldman Sachs", lei = "W22LROWP2IHZNBB6K528"},
}

for _, inst in ipairs(institutions) do
    Fleet.register_node(inst.name, inst.lei)
end

print("Fleet nodes registered:\n")
local status = Fleet.status()
print(string.format("  Total nodes: %d", status.total_nodes))
print(string.format("  Air-gapped: %s", status.air_gapped))
print()

-- Verify a party across the fleet
print("Verifying 'Acme Corp' across all 5 institutions:\n")
local results = Fleet.verify_party("Acme Corp", "529900TEST")
for node, result in pairs(results) do
    print(string.format("  %-20s: %s", node, result.verified and "VERIFIED" or "FAILED"))
end
print()

-- Broadcast KYC update
print("Broadcasting KYC status 'HIGH_RISK' for Acme Corp:\n")
local broadcast = Fleet.broadcast_kyc("Acme Corp", "HIGH_RISK")
print(string.format("  Broadcast to %d nodes", broadcast.nodes_updated))
print()

-- Final fleet status
local final_status = Fleet.status()
print("Final fleet status:")
print(string.format("  Nodes: %d", final_status.total_nodes))
print(string.format("  Mode: %s", final_status.air_gapped and "AIR-GAPPED (no cloud)" or "CONNECTED"))
