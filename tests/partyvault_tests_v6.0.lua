print("""
=== PARTYVAULT: WHAT IT DOES, HOW IT WORKS, WHY ===

WHAT IT DOES:
1. Ingests party data (Perl) — parses CSV/JSON, cleanses, deduplicates
2. Fingerprints identities (Zig) — BLAKE3 hash, Ed25519 attestation
3. Classifies risk (LuaJIT) — KYC rules, ML trust scores
4. Analyses quality (Julia) — completeness, anomalies
5. Distributes verification (Fleet) — Headscale mesh

HOW IT WORKS:
- Pipeline: Perl → Zig → LuaJIT → Julia
- Each language does what it does best
- ML trust scoring learns from labelled examples
- Fleet shares verifications across institutions
- Air-gapped: no cloud, no API keys

WHY IT MATTERS:
- March 2025: Euroclear digitised €15T Eurobond market
- Millions of parties need verifiable digital identities
- Enterprise MDM takes 6-12 months; PartyVault did it in 24 hours
- Open source: anyone can inspect, verify, improve
- MIT licensed: no vendor lock-in

THE KEY INSIGHT:
- Good parties (Deutsche Bank) get trust=100
- Bad parties (FTX) get trust=60 or lower
- ML learns from examples to improve accuracy
- Fleet ensures all institutions see the same verification""")
