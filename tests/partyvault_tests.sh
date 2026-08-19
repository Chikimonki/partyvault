#!/usr/bin/env bash
# ============================================================================
# PartyVault verification suite — run from the partyvault repo root:
#     ./tests/partyvault_tests.sh
#
# Convention: PASS / FAIL / KNOWN-FAIL. KNOWN-FAIL pins a documented defect
# until the fix lands — flip the expectation when fixed. Exit code:
# 0 if no UNEXPECTED failures, 1 otherwise.
# ============================================================================
set -u
cd "$(dirname "$0")/.." || exit 1

PASS=0; FAIL=0; KNOWNFAIL=0
ok()    { echo "  PASS  $1"; PASS=$((PASS+1)); }
bad()   { echo "  FAIL  $1"; FAIL=$((FAIL+1)); }
known() { echo "  KNOWN-FAIL  $1  (documented defect — flip when fixed)"; KNOWNFAIL=$((KNOWNFAIL+1)); }
check() { # check <description> <command...>
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then ok "$desc"; else bad "$desc"; fi
}

CLEANSED=./output/cleansed_parties.csv
REPORT=./output/ingestion_report.txt
IDENTITIES=./output/fingerprinted_parties.txt      # ADAPT if your IDENTITY output path differs
KEYS=./output/keys                      # ADAPT to wherever key material is stored

echo "=== PartyVault Verification Suite ==="
echo

# --- T01: pipeline artifacts exist ------------------------------------------
echo "[T01] Pipeline artifacts"
check "cleansed CSV exists"        test -f "$CLEANSED"
check "ingestion report exists"    test -f "$REPORT"
if [ -f "$IDENTITIES" ]; then ok "identities file exists"; else
  echo "  NOTE  $IDENTITIES not found — ADAPT path at top of this script"
fi

# --- T02: pinned ingestion counts (sample fixture) ---------------------------
echo "[T02] Ingestion counts (pinned fixture: 12 records)"
check "Valid: 11"      grep -q "Valid Records:       11" "$REPORT"
check "Rejected: 1"    grep -qE "Rejected Records:[[:space:]]+1" "$REPORT"
check "Duplicates: 1"  grep -qE "Duplicates Found:[[:space:]]+1" "$REPORT"
check "P007 rejected as empty" grep -q "P007: REJECTED" "$REPORT"

# --- T03: data quality score arithmetic --------------------------------------
echo "[T03] Quality score integrity"
if grep -q "Data Quality Score:  91.7%" "$REPORT"; then
  ok "score 91.7% == 11 valid / 12 total"
else bad "score does not match 11/12 = 91.7%"; fi

# --- T04: LEI validation oracle rows -----------------------------------------
echo "[T04] LEI oracle (pinned per-record verdicts)"
check "P001 Deutsche Bank LEI OK"            grep "^P001," "$CLEANSED" | grep -q ",1,OK,"
check "P003 invalid format detected"         grep "^P003," "$CLEANSED" | grep -q "INVALID_FORMAT"
check "P005 missing LEI detected"            grep "^P005," "$CLEANSED" | grep -q "MISSING"
check "P009 checksum failure detected"       grep "^P009," "$CLEANSED" | grep -q "CHECKSUM_FAIL"
check "P011 checksum failure detected"       grep "^P011," "$CLEANSED" | grep -q "CHECKSUM_FAIL"
check "P012 Goldman LEI OK"                  grep "^P012," "$CLEANSED" | grep -q ",1,OK,"

# --- T05: cross-layer consistency (KNOWN DEFECT) ------------------------------
echo "[T05] Cross-layer consistency: ingestion verdicts must reach the crypto layer"
if [ -f "$IDENTITIES" ]; then
  if grep "^IDENTITY|P009|" "$IDENTITIES" | grep -q "lei_valid=true|trust=100" ||
     grep "^IDENTITY|P011|" "$IDENTITIES" | grep -q "lei_valid=true|trust=100"; then
    known "P009/P011 checksum-failed LEIs attested as lei_valid=true/trust=100"
  else
    ok "checksum-failed LEIs no longer attested at full trust"
  fi
else echo "  SKIP  identities file not found"; fi

# --- T06: key persistence (KNOWN DEFECT) --------------------------------------
echo "[T06] Attestation key persistence"
PUB1=$(./run_demo.sh 2>/dev/null | grep -o "public=[0-9a-f]*" | head -1)
PUB2=$(./run_demo.sh 2>/dev/null | grep -o "public=[0-9a-f]*" | head -1)
if [ -z "$PUB1" ] || [ -z "$PUB2" ]; then
  echo "  SKIP  could not capture public keys from run_demo output"
elif [ "$PUB1" = "$PUB2" ]; then
  ok "public key stable across runs (attestation chain possible)"
else
  known "keypair regenerated each run — attestations unverifiable across runs"
fi

# --- T07: secret redaction -----------------------------------------------------
echo "[T07] Secret-key hygiene"
SECRET=$(./run_demo.sh 2>/dev/null | grep -o "secret=[0-9a-f]\{20,\}" | head -1 | cut -d= -f2)
if [ -n "$SECRET" ] && grep -rq "$SECRET" ./output/ 2>/dev/null; then
  bad "secret key material found in output artifacts"
else
  ok "no secret key material in output artifacts"
fi
check "logs redact secret" bash -c "./run_demo.sh 2>&1 | grep -q 'secret=REDACTED'"

# --- T08: signature round-trip (ADAPT) ----------------------------------------
echo "[T08] Sign/verify round-trip"
echo "  ADAPT  wire to your Zig verify entry point:"
echo "         sign fingerprint -> verify OK -> tamper -> verify MUST fail"

# --- T09: Unicode integrity -----------------------------------------------------
echo "[T09] Unicode integrity"
check "Müller survives cleansing byte-for-byte" grep -q "Jean-Pierre Müller & Associés" "$CLEANSED"

# --- T10: deduplication integrity ------------------------------------------------
echo "[T10] Dedup integrity"
K1=$(grep "^P001," "$CLEANSED" | cut -d, -f15)
K4=$(grep "^P004," "$CLEANSED" | cut -d, -f15)
if [ -n "$K1" ] && [ "$K1" = "$K4" ]; then
  ok "P001/P004 share dedup_key ($K1)"
else bad "P001/P004 dedup keys differ ($K1 vs $K4)"; fi
check "P004 marked DUPLICATE of P001" grep "^P004," "$CLEANSED" | grep -q "DUPLICATE:matches_P001"

# --- T11: pipe-injection resistance (KNOWN DEFECT UNTIL ESCAPING LANDS) ---------
echo "[T11] Field-injection resistance"
TMPFIX=$(mktemp); TMPOUT=$(mktemp)
printf 'id,legal_name,country,lei\nP900,Evil|Corp\\nINJECTED|LINE,,\n' > "$TMPFIX"
# ADAPT: point your ingestion at $TMPFIX; here we test the format contract:
if grep "^IDENTITY|" "$IDENTITIES" 2>/dev/null | awk -F'|' '{print NF}' | sort -u | wc -l | grep -q "^1$"; then
  ok "IDENTITY rows have uniform field counts (no injection visible)"
else
  known "IDENTITY field counts vary — pipe/newline escaping needed in party fields"
fi
rm -f "$TMPFIX" "$TMPOUT"

# --- T12/T13: ML determinism and bounds -----------------------------------------
echo "[T12/T13] ML trust scoring"
ML_SNIPPET='
package.path = "./lua/?.lua;" .. package.path
local M = require("ml_trust_score")
local p = {name="Test Bank", lei_valid=true, lei_status="OK", country="DE",
           entity_type="CREDIT_INSTITUTION", status="ACTIVE", email_valid=true}
local ex = {{party=p, actual_trust=95},
            {party={name="Bad", lei_valid=false, country="KY",
                    entity_type="SHELL_COMPANY", status="ACTIVE"}, actual_trust=5}}
M.train(ex)
local s1 = M.calculate(p); M.train(ex); local s2 = M.calculate(p)
print(s1 .. " " .. s2)'
R=$(luajit -e "$ML_SNIPPET" 2>/dev/null)
if [ -n "$R" ]; then
  S1=${R%% *}; S2=${R##* }
  if [ "$S1" = "$S2" ]; then ok "deterministic: same training -> same score ($S1)";
  else bad "non-deterministic scores: $S1 vs $S2"; fi
  # Use awk for decimal comparison (bash can't do floats)
  IN_BOUNDS=$(awk -v score="$S1" 'BEGIN { if (score >= 0 && score <= 100) print "yes"; else print "no" }')
  if [ "$IN_BOUNDS" = "yes" ]; then
    ok "score within [0,100]"; else bad "score out of bounds: $S1"; fi
else echo "  SKIP  ml_trust_score not reachable via luajit (ADAPT path)"; fi

# --- T14: ML learning signal ------------------------------------------------------
echo "[T14] ML learning signal (post-training error < naive baseline)"
echo "  ADAPT  assert avg training error decreases over epochs and report"
echo "         a HELD-OUT error (current runs report training-set error only)"

# --- T15: Julia stage presence ------------------------------------------------------
echo "[T15] Julia analytics stage"
echo "  ADAPT  assert the Julia quality artifact exists (path here), or remove"
echo "         the Julia claim from the README until wired"

# --- T16: README hygiene --------------------------------------------------------------
echo "[T16] README hygiene"
HEADINGS=$(grep -c "^## Roadmap" README.md 2>/dev/null || echo 0)
if [ "$HEADINGS" = "1" ]; then ok "single Roadmap heading"; else bad "duplicate Roadmap headings ($HEADINGS)"; fi
check "Euroclear date matches citation (2026)" grep -q "16 March 2026" README.md

echo
echo "=== Summary: PASS=$PASS  FAIL=$FAIL  KNOWN-FAIL=$KNOWNFAIL ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
