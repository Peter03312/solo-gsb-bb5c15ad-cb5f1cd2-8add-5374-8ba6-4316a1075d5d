#!/usr/bin/env bash
# End-to-end demo: submit a workflow, claim tasks, report results, cancel.
# Usage: ./scripts/demo.sh [BASE_URL]
set -euo pipefail

BASE="${1:-http://localhost:8000}"
KEY="demo-$(date +%s)"

echo "== 1. Submit workflow (extract -> transform -> load, independent audit) =="
SUBMIT=$(curl -s -X POST "$BASE/workflows" -H 'Content-Type: application/json' -d "{
  \"idempotency_key\": \"$KEY\",
  \"tasks\": [
    {\"task_id\": \"extract\",   \"max_attempts\": 3},
    {\"task_id\": \"transform\", \"depends_on\": [\"extract\"],   \"max_attempts\": 2},
    {\"task_id\": \"load\",      \"depends_on\": [\"transform\"], \"max_attempts\": 1},
    {\"task_id\": \"audit\",     \"max_attempts\": 1}
  ]
}")
WF=$(echo "$SUBMIT" | python3 -c "import sys,json;print(json.load(sys.stdin)['workflow_id'])")
echo "workflow_id = $WF"

echo; echo "== 2. Idempotent replay (same key + same request returns the original) =="
curl -s -o /dev/null -w "replay HTTP %{http_code}\n" -X POST "$BASE/workflows" \
  -H 'Content-Type: application/json' -d "{
  \"idempotency_key\": \"$KEY\",
  \"tasks\": [
    {\"task_id\": \"extract\",   \"max_attempts\": 3},
    {\"task_id\": \"transform\", \"depends_on\": [\"extract\"],   \"max_attempts\": 2},
    {\"task_id\": \"load\",      \"depends_on\": [\"transform\"], \"max_attempts\": 1},
    {\"task_id\": \"audit\",     \"max_attempts\": 1}
  ]
}"

echo; echo "== 3. Claim 'extract' (only dependency-free tasks are claimable) =="
CLAIM=$(curl -s -X POST "$BASE/workflows/$WF/claim" \
  -H 'Content-Type: application/json' -d '{"task_id": "extract", "lease_seconds": 60}')
echo "$CLAIM"
TOKEN=$(echo "$CLAIM" | python3 -c "import sys,json;print(json.load(sys.stdin)['lease_token'])")

echo; echo "== 4. Report success with the lease token =="
curl -s -o /dev/null -w "report HTTP %{http_code}\n" -X POST \
  "$BASE/workflows/$WF/tasks/extract/result" \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$TOKEN\", \"outcome\": \"success\"}"

echo; echo "== 5. 'transform' is now claimable; fail it once to show retry =="
CLAIM=$(curl -s -X POST "$BASE/workflows/$WF/claim" \
  -H 'Content-Type: application/json' -d '{"task_id": "transform"}')
TOKEN=$(echo "$CLAIM" | python3 -c "import sys,json;print(json.load(sys.stdin)['lease_token'])")
curl -s -o /dev/null -w "failure HTTP %{http_code}\n" -X POST \
  "$BASE/workflows/$WF/tasks/transform/result" \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$TOKEN\", \"outcome\": \"failure\"}"

echo; echo "== 6. Query workflow state =="
curl -s "$BASE/workflows/$WF" | python3 -m json.tool

echo; echo "== 7. Cancel the workflow (unfinished tasks become CANCELLED) =="
curl -s -X POST "$BASE/workflows/$WF/cancel" | python3 -c \
  "import sys,json;d=json.load(sys.stdin);print(d['status'],[(t['task_id'],t['status']) for t in d['tasks']])"
