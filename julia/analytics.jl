# Julia analytics for PartyVault — wired to pipeline output
# Reads fingerprinted_parties.txt and produces quality_report.json

using Statistics
using JSON
using DataFrames

function analyse_parties(input_file::String)
    println("[JULIA] Analysing party records...")
    
    # Read IDENTITY records
    identities = []
    open(input_file, "r") do f
        for line in eachline(f)
            if startswith(line, "IDENTITY|")
                parts = split(line, "|")
                push!(identities, Dict(
                    "party_id" => parts[2],
                    "name" => parts[3],
                    "country" => parts[4],
                    "trust" => parse(Float64, parts[end-1]),
                ))
            end
        end
    end
    
    if isempty(identities)
        return Dict("error" => "No identity records found")
    end
    
    # Compute quality metrics
    trust_scores = [id["trust"] for id in identities]
    mean_trust = mean(trust_scores)
    std_trust = std(trust_scores)
    min_trust = minimum(trust_scores)
    max_trust = maximum(trust_scores)
    
    # Anomaly detection (simple z-score)
    anomalies = []
    for id in identities
        z_score = (id["trust"] - mean_trust) / (std_trust > 0 ? std_trust : 1)
        if abs(z_score) > 2.0
            push!(anomalies, Dict(
                "party_id" => id["party_id"],
                "name" => id["name"],
                "trust" => id["trust"],
                "z_score" => z_score,
            ))
        end
    end
    
    # Completeness
    completeness = length(identities) / max(length(identities), 1) * 100
    
    result = Dict(
        "total_records" => length(identities),
        "mean_trust" => round(mean_trust, digits=2),
        "std_trust" => round(std_trust, digits=2),
        "min_trust" => min_trust,
        "max_trust" => max_trust,
        "completeness" => round(completeness, digits=1),
        "anomalies" => Dict(
            "total" => length(anomalies),
            "records" => anomalies,
        ),
        "timestamp" => string(Dates.now()),
    )
    
    return result
end

# Main entry point
if length(ARGS) >= 1
    input_file = ARGS[1]
    output_file = length(ARGS) >= 2 ? ARGS[2] : "output/quality_report.json"
    
    result = analyse_parties(input_file)
    
    open(output_file, "w") do f
        JSON.print(f, result, 4)
    end
    
    println("[JULIA] Quality report written to $output_file")
    println("[JULIA] Records: $(result["total_records"])")
    println("[JULIA] Anomalies: $(result["anomalies"]["total"])")
end
