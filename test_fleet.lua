package.path = "./lua/?.lua;" .. package.path

local Fleet = require("fleet")

print("=== PartyVault Fleet Test ===\n")

-- Register some financial institutions
Fleet.register_node({name = "Clearstream", lei = "5299000J2N45DDNE4Y28"})
Fleet.register_node({name = "Euroclear", lei = "549300OZ46M7KZ8I6D96"})
Fleet.register_node({name = "BNY Mellon", lei = "549300HY8H0Z7V7E8H44"})

print("1. Fleet nodes registered:")
local status = Fleet.status()
print(string.format("   Total: %d", status.total_nodes))
print(string.format("   Air-gapped: %s", tostring(status.air_gapped)))
print()

print("2. Verifying party across fleet:")
local results = Fleet.verify_party("Acme Corp", "529900TEST")
for node, result in pairs(results) do
    print(string.format("   %s: %s", node, result.verified and "VERIFIED" or "FAILED"))
end
print()

print("3. Broadcasting KYC update:")
local broadcast = Fleet.broadcast_kyc("Acme Corp", "HIGH_RISK")
print(string.format("   Broadcast to %d nodes", broadcast.nodes_updated))
print()

print("=== Fleet Test Complete ===")
