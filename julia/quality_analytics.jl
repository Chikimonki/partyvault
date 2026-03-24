#!/usr/bin/env julia
# ============================================================
# PartyVault Data Quality Analytics (Julia)
#
# Statistical profiling of party data:
#   - Completeness analysis
#   - Uniqueness / duplication detection
#   - Distribution analysis
#   - Anomaly detection
#   - Quality scoring and reporting
#
# Julia excels here: vectorized operations, statistical
# computing, and elegant data manipulation.
# ============================================================

using CSV
using DataFrames
using Statistics
using Printf
using Dates
using JSON

println(stderr, "[JULIA/ANALYTICS] PartyVault Data Quality Analytics starting...")

# ============================================================
# Configuration
# ============================================================

CLEANSED_FILE = get(ARGS, 1, "./output/cleansed_parties.csv")
RULES_OUTPUT  = get(ARGS, 2, "./output/rules_output.txt")  
REPORT_FILE   = "./output/quality_report.json"
SUMMARY_FILE  = "./output/quality_txt"

# ============================================================
# Data Loading
# ============================================================

println(stderr, "[JULIA/ANALYTICS] Loading cleansed data from: $CLEANSED_FILE")

df = CSV.read(CLEANSED_FILE, DataFrame; 
    missingstring=["", "NA", "N/A", "null"],
    types=Dict(
        :annual_revenue_eur => Union{Missing, Float64},
        :lei_valid => Union{Missing, Int64},
        :email_valid => Union{Missing, Int64},
        :country_valid => Union{Missing, Int64},
    )
)

n_records = nrow(df)
println(stderr, "[JULIA/ANALYTICS] Loaded $n_records records with $(ncol(df)) fields.")

# ============================================================
# 1. COMPLETENESS ANALYSIS
# ============================================================

println(stderr, "[JULIA/ANALYTICS] Running completeness analysis...")

struct FieldProfile
    field_name::String
    total::Int
    present::Int
    missing::Int
    completeness_pct::Float64
    unique_values::Int
    uniqueness_pct::Float64
end

function profile_field(df::DataFrame, col::Symbol)
    total = nrow(df)
    present = count(!ismissing, df[!, col])
    missing_count = total - present
    completeness = present / total * 100
    
    # Count unique non-missing values
    non_missing = collect(skipmissing(df[!, col]))
    unique_count = length(unique(non_missing))
    uniqueness = if present > 0 unique_count / present * 100 else 0.0 end
    
    return FieldProfile(
        string(col), total, present, missing_count,
        completeness, unique_count, uniqueness
    )
end

key_fields = [:id, :legal_name, :country, :lei, :entity_type, 
              :status, :address, :contact_email, :annual_revenue_eur]

profiles = [profile_field(df, col) for col in key_fields if hasproperty(df, col)]

# ============================================================
# 2. DISTRIBUTION ANALYSIS
# ============================================================

println(stderr, "[JULIA/ANALYTICS] Running distribution analysis...")

# Country distribution
country_dist = if hasproperty(df, :country)
    combine(
        groupby(dropmissing(df, :country), :country),
        nrow => :count
    ) |> x -> sort(x, :count, rev=true)
else
    DataFrame()
end

# Entity type distribution
entity_dist = if hasproperty(df, :entity_type)
    combine(
        groupby(dropmissing(df, :entity_type), :entity_type),
        nrow => :count
    ) |> x -> sort(x, :count, rev=true)
else
    DataFrame()
end

# Status distribution
status_dist = if hasproperty(df, :status)
    combine(
        groupby(dropmissing(df, :status), :status),
        nrow => :count
    ) |> x -> sort(x, :count, rev=true)
else
    DataFrame()
end

# ============================================================
# 3. ANOMALY DETECTION
# ============================================================

println(stderr, "[JULIA/ANALYTICS] Running anomaly detection...")

struct Anomaly
    party_id::String
    anomaly_type::String
    severity::String  # LOW, MEDIUM, HIGH, CRITICAL
    description::String
end

anomalies = Anomaly[]

for row in eachrow(df)
    pid = coalesce(row.id, "UNKNOWN")
    
    # Anomaly: Missing critical fields
    if ismissing(row.legal_name) || (row.legal_name isa String && strip(row.legal_name) == "")
        push!(anomalies, Anomaly(pid, "MISSING_NAME", "HIGH", "Party has no legal name"))
    end
    
    if ismissing(row.country) || (row.country isa String && strip(row.country) == "")
        push!(anomalies, Anomaly(pid, "MISSING_COUNTRY", "HIGH", "Party has no country code"))
    end
    
    # Anomaly: Invalid LEI
    if hasproperty(row, :lei_valid) && !ismissing(row.lei_valid) && row.lei_valid == 0
        lei_val = coalesce(row.lei, "MISSING")
        if lei_val != "" && lei_val != "MISSING"
            push!(anomalies, Anomaly(pid, "INVALID_LEI", "HIGH", "LEI present but invalid: $lei_val"))
        else
            push!(anomalies, Anomaly(pid, "MISSING_LEI", "MEDIUM", "No LEI provided"))
        end
    end
    
    # Anomaly: Revenue outliers (if available)
    if hasproperty(row, :annual_revenue_eur) && !ismissing(row.annual_revenue_eur)
        rev = row.annual_revenue_eur
        if rev == 0
            push!(anomalies, Anomaly(pid, "ZERO_REVENUE", "MEDIUM", "Entity reports zero revenue"))
        elseif rev > 100_000_000_000  # > 100B
            push!(anomalies, Anomaly(pid, "EXTREME_REVENUE", "LOW", "Revenue exceeds €100B — verify"))
        end
    end
    
    # Anomaly: Duplicate detection (by dedup_key)
    if hasproperty(row, :cleansing_notes) && !ismissing(row.cleansing_notes)
        notes = string(row.cleansing_notes)
        if occursin("DUPLICATE", notes)
            push!(anomalies, Anomaly(pid, "DUPLICATE_RECORD", "HIGH", "Potential duplicate: $notes"))
        end
        if occursin("HIGH_RISK", notes)
            push!(anomalies, Anomaly(pid, "HIGH_RISK_FLAG", "CRITICAL", "High risk flag: $notes"))
        end
    end
end

# ============================================================
# 4. QUALITY SCORING
# ============================================================

println(stderr, "[JULIA/ANALYTICS] Computing quality scores...")

# Overall data quality score (0-100)
function compute_quality_score(profiles, anomalies, n_records)
    # Completeness component (40% weight)
    avg_completeness = mean([p.completeness_pct for p in profiles])
    completeness_score = avg_completeness * 0.4
    
    # Validity component (30% weight) — based on LEI and country validation
    lei_valid_count = if hasproperty(df, :lei_valid)
        count(x -> !ismissing(x) && x == 1, df.lei_valid)
    else 0 end
    country_valid_count = if hasproperty(df, :country_valid)
        count(x -> !ismissing(x) && x == 1, df.country_valid)
    else 0 end
    validity_pct = (lei_valid_count + country_valid_count) / (2 * n_records) * 100
    validity_score = validity_pct * 0.3
    
    # Anomaly component (30% weight) — fewer anomalies = higher score
    anomaly_rate = length(anomalies) / max(n_records, 1)
    anomaly_score = max(0, (1 - anomaly_rate)) * 30
    
    return min(completeness_score + validity_score + anomaly_score, 100.0)
end

quality_score = compute_quality_score(profiles, anomalies, n_records)

# Severity distribution
severity_counts = Dict(
    "LOW" => count(a -> a.severity == "LOW", anomalies),
    "MEDIUM" => count(a -> a.severity == "MEDIUM", anomalies),
    "HIGH" => count(a -> a.severity == "HIGH", anomalies),
    "CRITICAL" => count(a -> a.severity == "CRITICAL", anomalies),
)

# ============================================================
# 5. GENERATE REPORTS
# ============================================================

println(stderr, "[JULIA/ANALYTICS] Generating reports...")

# JSON report for programmatic consumption
report = Dict(
    "generated_at" => Dates.format(now(), "yyyy-mm-dd HH:MM:SS"),
    "total_records" => n_records,
    "quality_score" => round(quality_score, digits=1),
    "completeness" => Dict(
        p.field_name => Dict(
            "present" => p.present,
            "missing" => p.missing,
            "completeness_pct" => round(p.completeness_pct, digits=1),
            "unique_values" => p.unique_values,
            "uniqueness_pct" => round(p.uniqueness_pct, digits=1)
        )
        for p in profiles
    ),
    "distributions" => Dict(
        "by_country" => [Dict("country" => r.country, "count" => r.count) for r in eachrow(country_dist)],
        "by_entity_type" => [Dict("type" => r.entity_type, "count" => r.count) for r in eachrow(entity_dist)],
        "by_status" => [Dict("status" => r.status, "count" => r.count) for r in eachrow(status_dist)],
    ),
    "anomalies" => Dict(
        "total" => length(anomalies),
        "by_severity" => severity_counts,
        "details" => [
            Dict(
                "party_id" => a.party_id,
                "type" => a.anomaly_type,
                "severity" => a.severity,
                "description" => a.description
            )
            for a in anomalies
        ]
    ),
)

open(REPORT_FILE, "w") do io
    JSON.print(io, report, 2)
end

# Human-readable summary
human_summary = """
╔══════════════════════════════════════════════════════════════╗
║          PartyVault Data Quality Analytics Report           ║
╠══════════════════════════════════════════════════════════════╣
║  Generated: $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))
║  Records Analyzed: $n_records
║                                                              
║  ┌─── OVERALL QUALITY SCORE ───┐                            
║  │                              │                            
║  │      $(@sprintf("%.1f", quality_score)) / 100.0              │
║  │      $(quality_score >= 80 ? "✅ GOOD" : quality_score >= 60 ? "⚠️  FAIR" : "❌ POOR")                       │
║  │                              │                            
║  └──────────────────────────────┘                            
║                                                              
║  COMPLETENESS:                                               
"""

for p in profiles
    indicator = p.completeness_pct >= 90 ? "✅" : p.completeness_pct >= 70 ? "⚠️ " : "❌"
    global human_summary
    human_summary *= "║    $indicator $(rpad(p.field_name, 25)) $(@sprintf("%5.1f%%", p.completeness_pct))  ($(p.present)/$(p.total))\n"
end

human_summary *= """║                                                              
║  ANOMALIES:                                                  
║    CRITICAL: $(severity_counts["CRITICAL"])
║    HIGH:     $(severity_counts["HIGH"])
║    MEDIUM:   $(severity_counts["MEDIUM"])
║    LOW:      $(severity_counts["LOW"])
║    TOTAL:    $(length(anomalies))
║                                                              
║  DISTRIBUTIONS:                                              
║    Countries: $(nrow(country_dist))  |  Entity Types: $(nrow(entity_dist))  |  Statuses: $(nrow(status_dist))
║                                                              
╚══════════════════════════════════════════════════════════════╝
"""

open(SUMMARY_FILE, "w") do io
    write(io, human_summary)
end

# Print summary to stdout
print(human_summary)

# Print anomalies detail to stderr for pipeline visibility
for a in anomalies
    severity_color = Dict("CRITICAL" => "🔴", "HIGH" => "🟠", "MEDIUM" => "🟡", "LOW" => "🔵")
    icon = get(severity_color, a.severity, "⚪")
    println(stderr, "[JULIA/ANALYTICS]   $icon $(a.party_id): $(a.anomaly_type) — $(a.description)")
end

println(stderr, "[JULIA/ANALYTICS] Quality score: $(@sprintf("%.1f", quality_score))/100")
println(stderr, "[JULIA/ANALYTICS] Reports written to: $REPORT_FILE, $SUMMARY_FILE")
println(stderr, "[JULIA/ANALYTICS] Analytics complete.")
