# 🔐 PartyVault — Cryptographic Party Identity Service

**A proof-of-concept demonstrating cryptographic identity management for 
financial market infrastructure, built with a polyglot stack optimized 
for each layer's strengths.**

# PartyVault

> **Built in 24 hours** to demonstrate cryptographic party identity management for post-digitization capital markets.  
> **Stack:** Perl (data cleansing) → Zig (crypto) → LuaJIT (rules) → Julia (analytics)  
> **Each language doing what it does best.**

Videos:
1 https://youtu.be/3KLy4c-r-w8
2 https://youtu.be/tlr9CJlRpjU

## Why This Exists

On March 16, 2025, Euroclear and Clearstream digitized the €15 trillion Eurobond market. 
That means millions of parties (issuers, investors, agents, custodians) now need cryptographically 
verifiable digital identities, real-time KYC classification, and automated regulatory compliance.

**Enterprise MDM platforms (Informatica, Reltio, IBM InfoSphere) take 6-12 months to implement 
for a use case like this.**

**I built a working prototype in one day** to show what's possible when you choose the right 
tool for each layer instead of forcing everything into a single-language stack.

This isn't production-ready. It's a demonstration of:
- Systems-level thinking across paradigms
- Performance-aware architecture (Zig + LuaJIT)
- Data-native engineering (Perl + Julia)
- Rapid prototyping without sacrificing correctness

If your team has a hard problem that requires unconventional thinking, [let's talk](mailto:pan283@gmail.com).

## The Stack — Each Language Where It Excels

| Layer | Language | Purpose |
|-------|----------|---------|
| **Data Ingestion & Cleansing** | **Perl** | Multi-format parsing, regex-powered normalization, LEI validation, deduplication |
| **Cryptographic Identity** | **Zig** | BLAKE3 fingerprinting, Ed25519 attestation, zero-GC deterministic processing |
| **Regulatory Classification** | **LuaJIT** | Hot-swappable business rules, KYC classification, risk scoring, sanctions screening |
| **Quality Analytics** | **Julia** | Statistical profiling, anomaly detection, completeness analysis, quality scoring |

## Why This Stack?

In a world where a €15T+ Eurobond market just went digital (March 2025), 
party identity infrastructure must be:

- **Fast** — Zig and LuaJIT deliver near-C performance without GC pauses
- **Correct** — Zig's compile-time safety, Julia's type system, Perl's battle-tested text processing
- **Adaptable** — LuaJIT rules can be updated by compliance teams without redeployment
- **Analytical** — Julia provides statistical rigor for data quality that SQL dashboards can't match

<<<<<<< HEAD
"Clearstream and Euroclear Digitize Eurobond Issuance Revolutionizing the Market - by TABEA BEHR, 16.03.2026" - https://www.clearstream.com/clearstream-en/newsroom/260316-5012146 

## Roadmap

=======
https://www.clearstream.com/clearstream-en/newsroom/260316-5012146 - "Clearstream and Euroclear Digitize Eurobond Issuance Revolutionizing the Market - by TABEA BEHR, 16.03.2026"

## Roadmap

### v0.2 (Q2 2025)
- [ ] REST API (OpenResty + LuaJIT)
- [ ] Live GLEIF LEI lookup integration
- [ ] EU Consolidated Sanctions List screening
- [ ] SQLite persistent storage
- [ ] Multi-tenant support

### v0.3 (Q3 2025)
- [ ] eIDAS 2.0 verifiable credential issuance
- [ ] Real-time change detection
- [ ] Webhook notifications
- [ ] PostgreSQL backend option
- [ ] Docker Compose multi-node deployment

>>>>>>> 0573f14 (Add ci.yml and other changes)
### v1.0 (Q4 2025)
- [ ] Production-grade security audit
- [ ] SOC 2 Type II compliance
- [ ] Enterprise SLA support
- [ ] MiCA compliance toolkit

## Quick Start

```bash
# Setup (WSL/Ubuntu)
chmod +x setup.sh run_demo.sh
./setup.sh

# Run the full pipeline
./run_demo.sh
