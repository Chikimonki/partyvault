# ICM: PartyVault v9.0 — Production Stack

## Context
Cryptographic identity for post-Euroclear digital markets.
19 PASS / 0 FAIL / 0 KNOWN-FAIL. Production-honest.

## Architecture
Perl (cleansing) → Zig (crypto) → LuaJIT (rules + ML) → Julia (analytics)
↓ ↓ ↓ ↓
Data quality BLAKE3 + Ed25519 KYC + trust scores Anomalies

## ML Components
- `lua/ml_trust_score.lua` — weighted feature model, deterministic
- `julia/analytics.jl` — statistical profiling, anomaly detection
- Two-stage trust: Stage 1 (Zig rules) + Stage 2 (LuaJIT ML)

## Production Features (v9.0)
- Docker Compose orchestration
- Multi-tenant isolation
- eIDAS 2.0 production certificates
- SOC 2 Type II preparation

## Verification
```bash
./tests/partyvault_tests.sh
Result: 19 PASS / 0 FAIL / 0 KNOWN-FAIL

Data Flow
Perl: parse CSV/JSON, cleanse, validate LEI

Zig: BLAKE3 fingerprint, Ed25519 attestation

LuaJIT: KYC rules, ML trust scores

Julia: quality analytics, anomalies

Fleet: Headscale mesh distribution

Key Files
zig/src/main.zig — crypto core

lua/rule_engine.lua — KYC rules

lua/ml_trust_score.lua — ML scoring

julia/analytics.jl — quality analytics

api/ — REST API, GLEIF, sanctions, storage

docker-compose.yml — orchestration

Status
Version: 9.0

License: MIT

Verification: Archive-ready

Production: Near-ready (SOC 2 pending)
