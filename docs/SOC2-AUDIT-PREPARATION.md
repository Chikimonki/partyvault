# SOC 2 Type II Audit Preparation

## Control Areas

### Security
- Access control: multi-tenant isolation implemented
- Encryption: Ed25519 for attestation, BLAKE3 for fingerprinting
- Key management: persistent keypair with 0600 permissions

### Availability
- Docker Compose orchestration
- PostgreSQL replication ready
- Redis for high-throughput streaming

### Processing Integrity
- 19-test verification suite (0 FAIL)
- Deterministic ML scoring
- Cross-layer consistency validated

### Confidentiality
- Secret keys redacted in all logs
- Tenant isolation in multi-tenant mode
- Air-gapped capable (no cloud dependency)

### Privacy
- PII minimised (LEI, not personal data)
- Data retention configurable
- GDPR-compliant design

## Audit Evidence Available
- `tests/partyvault_tests.sh` — verification suite
- `output/ingestion_report.txt` — data quality
- `output/quality_report.json` — Julia analytics
- `output/keypair.txt` — persistent keys (0600)

## Next Steps
1. Formal risk assessment
2. Penetration testing
3. Independent audit firm engagement
4. SOC 2 Type I (point-in-time)
5. SOC 2 Type II (over 6 months)
