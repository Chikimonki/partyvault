local Fleet = {
    nodes = {},
    headscale_url = "http://127.0.0.1:8080",
    air_gapped = true,
    policy = "default",
}

function Fleet.register_node(node)
    Fleet.nodes[node.name] = {
        name = node.name or "Unknown",
        lei = node.lei or "?",
        headscale_ip = node.headscale_ip or "100.64.0.1",
        status = "PENDING",
        last_seen = os.time(),
        parties_verified = 0,
    }
    return Fleet.nodes[node.name]
end

-- Count nodes properly (string keys don't work with #)
function Fleet.count_nodes()
    local count = 0
    for _ in pairs(Fleet.nodes) do count = count + 1 end
    return count
end

function Fleet.verify_party(party_name, party_lei)
    local results = {}
    
    for node_name, node in pairs(Fleet.nodes) do
        results[node_name] = {
            verified = math.random() > 0.1,
            timestamp = os.time(),
            node = node_name,
        }
    end
    
    return results
end

function Fleet.broadcast_kyc(party_name, kyc_status)
    local count = 0
    for node_name, node in pairs(Fleet.nodes) do
        node.last_kyc_update = {
            party = party_name,
            status = kyc_status,
            timestamp = os.time(),
        }
        count = count + 1
    end
    
    return {
        broadcast = true,
        nodes_updated = count,
    }
end

function Fleet.status()
    local total = Fleet.count_nodes()
    local online = 0
    for _, node in pairs(Fleet.nodes) do
        if node.status == "ACTIVE" then online = online + 1 end
    end
    
    return {
        total_nodes = total,
        online_nodes = online,
        air_gapped = Fleet.air_gapped,
        headscale = Fleet.headscale_url,
    }
end

return Fleet
