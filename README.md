# 🔐 PartyVault — Cryptographic Party Identity Service

**A proof-of-concept demonstrating cryptographic identity management for 
financial market infrastructure, built with a polyglot stack optimized 
for each layer's strengths.**

## The Stack — Each Language Where It Excels

| Layer | Language | Purpose |
|-------|----------|---------|
| **Data Ingestion & Cleansing** | **Perl** | Multi-format parsing, regex-powered normalization, LEI validation, deduplication |
| **Cryptographic Identity** | **Zig** | BLAKE3 fingerprinting, Ed25519 attestation, zero-GC deterministic processing |
| **Regulatory Classification** | **LuaJIT** | Hot-swappable business rules, KYC classification, risk scoring, sanctions screening |
| **Quality Analytics** | **Julia** | Statistical profiling, anomaly detection, completeness analysis, quality scoring |

## Why This Stack?

In a world where a €15T+ Eurobond market just went digital (March 2025), https://www.clearstream.com/clearstream-en/newsroom/260316-5012146 - "Clearstream and Euroclear Digitize Eurobond Issuance Revolutionizing the Market - by TABEA BEHR, 16.03.2026",
party identity infrastructure must be:

- **Fast** — Zig and LuaJIT deliver near-C performance without GC pauses
- **Correct** — Zig's compile-time safety, Julia's type system, Perl's battle-tested text processing
- **Adaptable** — LuaJIT rules can be updated by compliance teams without redeployment
- **Analytical** — Julia provides statistical rigor for data quality that SQL dashboards can't match

Videos:
Run through - https://youtu.be/3KLy4c-r-w8
Walk through - https://youtu.be/tlr9CJlRpjU
## Quick Start

```bash
# Setup (WSL/Ubuntu)
chmod +x setup.sh run_demo.sh
./setup.sh

# Run the full pipeline
./run_demo.sh
