🔐 PartyVault — Cryptographic Party Identity Service
<<<<<<< HEAD
Version 6.0 · MIT Licensed · Open Source · Production-honest, not production-hardened.

Cryptographic identity management for financial market infrastructure: a polyglot pipeline where each layer runs in the language best suited to it — Perl (cleansing) → Zig (crypto) → LuaJIT (rules & ML) → Julia (analytics).

Videos: 1 · 2

Why This Exists
On 16 March 2026, Euroclear and Clearstream digitised the €15 trillion Eurobond market (Clearstream newsroom, 16.03.2026). Millions of parties — issuers, investors, agents, custodians — now require cryptographically verifiable digital identities, real-time KYC classification, and automated regulatory compliance.

Enterprise MDM platforms typically take 6–12 months to implement for this scale. PartyVault's first working prototype was built in 24 hours to demonstrate what is possible when each layer uses the right tool instead of forcing everything into one stack.

Verification Status
18 PASS / 0 FAIL / 1 KNOWN-FAIL — archive-ready verification suite.
=======

**Version 6.0** · MIT Licensed · Open Source · Production-honest, not production-hardened.

Cryptographic identity management for financial market infrastructure: a polyglot pipeline where each layer runs in the language best suited to it — Perl (cleansing) → Zig (crypto) → LuaJIT (rules & ML) → Julia (analytics).

**Videos**: [1](https://youtu.be/3KLy4c-r-w8) · [2](https://youtu.be/tlr9CJlRpjU)
>>>>>>> c9c09ed2d57eff87c885322828e09c01250cee80

Category	Count
Passing tests	18
Unexpected failures	0
Documented known defects	1 (key persistence)
Future work items	3 (sign/verify, held-out ML, Julia path)
The Stack
Layer	Language	Purpose
Data ingestion & cleansing	Perl	Multi-format parsing, regex normalisation, LEI validation, deduplication
Cryptographic identity	Zig	BLAKE3 fingerprinting, Ed25519 attestation, zero-GC deterministic processing
Regulatory classification	LuaJIT	Hot-swappable business rules, KYC classification, ML trust scores
Quality analytics	Julia	Statistical profiling, anomaly detection, completeness analysis
Pipeline
text

<<<<<<< HEAD
CSV/JSON parties
   │  Perl: parse → cleanse → validate LEI → deduplicate
   ▼
cleansed_parties.csv
   │  Zig: BLAKE3 fingerprint + Ed25519 attestation
   ▼
fingerprinted_parties.txt
   │  LuaJIT: KYC rules + ML trust score
   ▼
risk classifications
   │  Julia: completeness / anomaly analytics
   ▼
quality report
Trust Model
Stage 1 — Identity trust (Zig, rule-based). Valid LEI + valid country + acceptable entity type ⇒ high trust. Degraded by shell-company indicators, missing LEI, suspended status.

Stage 2 — Learned trust (LuaJIT ML). Weighted feature model (LEI validity, country risk, entity type, status, email, historical penalty) trained on labelled examples. Ranks parties for review priority.

Cross-layer integrity is pinned by test T05: a validation verdict at ingestion must reach the attestation layer — checksum-failed LEIs are never attested at full trust.

Verification
Bash

./tests/partyvault_tests.sh
The suite runs 16 checks: pinned ingestion counts, LEI oracle rows, cross-layer consistency, key persistence (documented defect), secret redaction, Unicode integrity, deduplication, injection resistance, ML determinism and bounds, and README hygiene.

Quick Start
Bash

chmod +x setup.sh run_demo.sh
./setup.sh
./run_demo.sh
## Roadmap

**v6.1 (next)**
- Persistent Ed25519 keystore (fixes T06)
- Sign/verify round-trip with tamper-must-fail test (T08)
- Held-out ML evaluation (T14)

**v7.0**
- REST API (OpenResty + LuaJIT)
- Live GLEIF LEI lookup (optional online mode)
- EU Consolidated Sanctions List screening
- SQLite persistent storage

**v8.0**
- eIDAS 2.0 verifiable credential issuance
- Real-time change detection, webhook notifications
- PostgreSQL backend option
=======
On 16 March 2026, Euroclear and Clearstream digitised the €15 trillion Eurobond market ([Clearstream newsroom, 16.03.2026](https://www.clearstream.com/clearstream-en/newsroom/260316-5012146)), ([Euroeclear newsroom, 16.03.2026](https://www.euroclear.com/newsandinsights/en/press/2026/mr-10-euroclear-clearstream-digitise-eurobond-issuance.html).). Millions of parties — issuers, investors, agents, custodians — now require cryptographically verifiable digital identities, real-time KYC classification, and automated regulatory compliance.

"The launch of our dematerialised issuance service marks a pivotal moment for the Eurobond market." -  Isabelle Delorme, Head of Product Strategy and Innovation, Euroclear

Enterprise MDM platforms typically take 6–12 months to implement for this scale. PartyVault's first working prototype was built in 24 hours to demonstrate what is possible when each layer uses the right tool instead of forcing everything into one stack.

## Verification Status

**18 PASS / 0 FAIL / 1 KNOWN-FAIL** — archive-ready verification suite.

| Category | Count |
|----------|-------|
| Passing tests | 18 |
| Unexpected failures | 0 |
| Documented known defects | 1 (key persistence) |
| Future work items | 3 (sign/verify, held-out ML, Julia path) |

## The Stack

| Layer | Language | Purpose |
|-------|----------|---------|
| Data ingestion & cleansing | Perl | Multi-format parsing, regex normalisation, LEI validation, deduplication |
| Cryptographic identity | Zig | BLAKE3 fingerprinting, Ed25519 attestation, zero-GC deterministic processing |
| Regulatory classification | LuaJIT | Hot-swappable business rules, KYC classification, ML trust scores |
| Quality analytics | Julia | Statistical profiling, anomaly detection, completeness analysis |

## Pipeline
CSV/JSON parties
│ Perl: parse → cleanse → validate LEI → deduplicate
▼
cleansed_parties.csv
│ Zig: BLAKE3 fingerprint + Ed25519 attestation
▼
fingerprinted_parties.txt
│ LuaJIT: KYC rules + ML trust score
▼
risk classifications
│ Julia: completeness / anomaly analytics
▼
quality report

text

## Trust Model

**Stage 1 — Identity trust (Zig, rule-based).** Valid LEI + valid country + acceptable entity type ⇒ high trust. Degraded by shell-company indicators, missing LEI, suspended status.

**Stage 2 — Learned trust (LuaJIT ML).** Weighted feature model (LEI validity, country risk, entity type, status, email, historical penalty) trained on labelled examples. Ranks parties for review priority.

**Cross-layer integrity is pinned by test T05**: a validation verdict at ingestion must reach the attestation layer — checksum-failed LEIs are never attested at full trust.

## Verification

```bash
./tests/partyvault_tests.sh

The suite runs 16 checks: pinned ingestion counts, LEI oracle rows, cross-layer consistency, key persistence (documented defect), secret redaction, Unicode integrity, deduplication, injection resistance, ML determinism and bounds, and README hygiene.

Quick Start
bash
chmod +x setup.sh run_demo.sh
./setup.sh
./run_demo.sh
>>>>>>> c9c09ed2d57eff87c885322828e09c01250cee80

Known Limitations
Documented, not hidden:

Key persistence — demo generates a fresh Ed25519 keypair per run. Persistent keystore required for verifiable attestation chains.
<<<<<<< HEAD
ML training set — small label set; scores are decision-support, not authoritative KYC verdicts.
Sign/verify round-trip — not yet wired to a tamper-must-fail test.
Field escaping — pipe/newline escaping for party names in progress.
=======

ML training set — small label set; scores are decision-support, not authoritative KYC verdicts.

Sign/verify round-trip — not yet wired to a tamper-must-fail test.

Field escaping — pipe/newline escaping for party names in progress.

>>>>>>> c9c09ed2d57eff87c885322828e09c01250cee80
Regulatory Relevance
PartyVault's verified party register and attestation chain align with the direction of travel in financial regulation — KYC/AML evidence automation, and the register-of-information discipline familiar from DORA Article 28 for ICT third parties. PartyVault assists evidence generation; compliance decisions remain with the regulated entity.

License & Provenance
MIT. Built on open standards: ISO 17442 (LEI), Ed25519 (RFC 8032), BLAKE3.
<<<<<<< HEAD

Built in Liverpool. Open source. Audit-ready verification.
=======
>>>>>>> c9c09ed2d57eff87c885322828e09c01250cee80
