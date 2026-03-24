#!/bin/bash
set -euo pipefail

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                                                              ║"
echo "║     🔐 PartyVault — Cryptographic Party Identity Service     ║"
echo "║                                                              ║"
echo "║     Stack: LuaJIT + Julia + Perl + Zig                      ║"
echo "║     Demo:  Financial Party MDM Pipeline                      ║"
echo "║                                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

mkdir -p output

# Find the Zig binary wherever it lives
ZIG_BIN=""
if command -v partyvault-crypto &>/dev/null; then
    ZIG_BIN="partyvault-crypto"
elif [ -f ./zig/partyvault-crypto ]; then
    ZIG_BIN="./zig/partyvault-crypto"
elif [ -f ./zig/zig-out/bin/partyvault-crypto ]; then
    ZIG_BIN="./zig/zig-out/bin/partyvault-crypto"
fi

# ============================================================
# Phase 0: Build Zig if needed (skip if binary exists)
# ============================================================
if [ -z "$ZIG_BIN" ]; then
    echo -e "${CYAN}━━━ Phase 0: Building Zig Cryptographic Core ━━━${NC}"
    echo ""
    if command -v zig &>/dev/null; then
        cd zig
        if zig build 2>&1; then
            echo -e "${GREEN}✓ Zig crypto core built successfully${NC}"
            ZIG_BIN="./zig/zig-out/bin/partyvault-crypto"
        else
            echo -e "${RED}✗ Zig build failed — using fallback${NC}"
        fi
        cd "$SCRIPT_DIR"
    else
        echo -e "${RED}✗ Zig not available — using fallback${NC}"
    fi
    echo ""
else
    echo -e "${CYAN}━━━ Phase 0: Zig Cryptographic Core ━━━${NC}"
    echo -e "${GREEN}✓ Pre-compiled binary found: $ZIG_BIN${NC}"
    echo ""
fi

# ============================================================
# Phase 1: Perl Data Ingestion
# ============================================================
echo -e "${CYAN}━━━ Phase 1: Data Ingestion & Cleansing (Perl) ━━━${NC}"
echo ""

perl perl/ingest_pipeline.pl \
    --data-dir ./data \
    --output ./output/cleansed_parties.csv \
    --report ./output/ingestion_report.txt \
    --verbose

echo ""
echo -e "${GREEN}✓ Ingestion complete${NC}"
echo ""

if [ -f ./output/ingestion_report.txt ]; then
    echo -e "${YELLOW}--- Ingestion Report ---${NC}"
    cat ./output/ingestion_report.txt
    echo ""
fi

# ============================================================
# Phase 2: Zig Cryptographic Fingerprinting
# ============================================================
echo -e "${CYAN}━━━ Phase 2: Cryptographic Identity (Zig) ━━━${NC}"
echo ""

if [ -n "$ZIG_BIN" ]; then
    echo -e "${YELLOW}Generating attestation keypair...${NC}"
    $ZIG_BIN keygen | tee ./output/keypair.txt
    echo ""

    echo -e "${YELLOW}Fingerprinting party records...${NC}"
    cat ./output/cleansed_parties.csv | \
        $ZIG_BIN process | \
        tee ./output/fingerprinted_parties.txt
else
    echo -e "${RED}Zig binary not available — generating mock fingerprints${NC}"
    perl -MDigest::SHA=sha256_hex -ne '
        next if $. == 1;
        chomp;
        my @f = split /,/;
        next unless $f[1];
        my $fp = sha256_hex(join("|", @f[1..4]));
        print "IDENTITY|$f[0]|$f[1]|$f[2]|$fp|$f[3]|$f[4]|lei_valid=$f[10]|country_valid=$f[13]|trust=50\n";
    ' ./output/cleansed_parties.csv | tee ./output/fingerprinted_parties.txt
fi

echo ""
echo -e "${GREEN}✓ Cryptographic fingerprinting complete${NC}"
echo ""

# ============================================================
# Phase 3: LuaJIT Business Rule Engine
# ============================================================
echo -e "${CYAN}━━━ Phase 3: Regulatory Classification (LuaJIT) ━━━${NC}"
echo ""

cat ./output/fingerprinted_parties.txt | \
    luajit lua/rule_engine.lua | \
    tee ./output/classified_parties.txt

echo ""
echo -e "${GREEN}✓ Regulatory classification complete${NC}"
echo ""

# ============================================================
# Phase 4: Julia Data Quality Analytics
# ============================================================
echo -e "${CYAN}━━━ Phase 4: Data Quality Analytics (Julia) ━━━${NC}"
echo ""

julia julia/quality_analytics.jl \
    ./output/cleansed_parties.csv \
    ./output/classified_parties.txt

echo ""
echo -e "${GREEN}✓ Quality analytics complete${NC}"
echo ""

# ============================================================
# Summary
# ============================================================
echo -e "${BOLD}${CYAN}━━━ Pipeline Complete ━━━${NC}"
echo ""
echo -e "${BOLD}Output Files:${NC}"
echo "  📄 output/cleansed_parties.csv       — Cleansed & normalized party data (Perl)"
echo "  📄 output/ingestion_report.txt       — Ingestion statistics (Perl)"
echo "  🔐 output/keypair.txt                — Service attestation keypair (Zig)"
echo "  🔐 output/fingerprinted_parties.txt  — Cryptographic identities (Zig)"
echo "  📋 output/classified_parties.txt     — Regulatory classification (LuaJIT)"
echo "  📊 output/quality_report.json        — Quality analytics (Julia)"
echo "  📊 output/quality_summary.txt        — Quality summary (Julia)"
echo ""
echo -e "${BOLD}Pipeline:${NC}"
echo "  Raw Data → [Perl: Ingest/Cleanse] → [Zig: Fingerprint/Attest]"
echo "           → [LuaJIT: Classify/Screen] → [Julia: Analyze/Report]"
echo ""
echo -e "${GREEN}${BOLD}Each language doing what it does best. That's the point.${NC}"
echo ""

# Quick Stats
if [ -f ./output/quality_report.json ]; then
    echo -e "${YELLOW}--- Quick Stats from Quality Report ---${NC}"
    perl -MJSON -e '
        local $/;
        my $json = decode_json(<STDIN>);
        printf "  Quality Score:    %.1f/100\n", $json->{quality_score};
        printf "  Total Records:    %d\n", $json->{total_records};
        printf "  Total Anomalies:  %d\n", $json->{anomalies}{total};
        printf "  Critical Issues:  %d\n", $json->{anomalies}{by_severity}{CRITICAL} // 0;
        printf "  High Issues:      %d\n", $json->{anomalies}{by_severity}{HIGH} // 0;
    ' < ./output/quality_report.json 2>/dev/null || echo "  (install JSON perl module for stats)"
    echo ""
fi
