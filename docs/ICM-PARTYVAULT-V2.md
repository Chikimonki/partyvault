# ICM: PartyVault v2 — ML Trust + Fleet

## Context
Cryptographic identity for post-Euroclear digital markets.
Built with Zig crypto, LuaJIT rules, Julia analytics, Perl ingestion.

## New Features
- **ML Trust Scoring**: `lua/ml_trust_score.lua`
  - Learns from labelled examples
  - Deutsche Bank: 72→76 after training
  - Shady Offshore: suppressed at 3
- **Fleet**: `lua/fleet.lua`
  - Headscale mesh networking
  - 3-node distributed verification
  - Air-gapped capable

## Pipeline
Raw Data → Perl → Zig Crypto → LuaJIT Rules + ML → Fleet → Julia Analytics

text

## Test Commands
```bash
luajit test_fleet.lua
luajit test_ml_trust.lua
./run_demo.sh
Status
CI green

12 party records processed

91.7% data quality
