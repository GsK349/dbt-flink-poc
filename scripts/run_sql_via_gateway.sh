#!/bin/bash
# Submits a SQL file to the local Flink SQL Gateway REST API.
# Usage:
#   run_sql_via_gateway <s3-key>          # downloads s3://sai-flink-flink-artifacts/<key>
#   run_sql_via_gateway --file /path.sql  # uses local file
set -uo pipefail

GATEWAY="${GATEWAY:-http://localhost:8083}"
ARTIFACT_BUCKET="${ARTIFACT_BUCKET:-sai-flink-flink-artifacts}"
AWS_REGION="${AWS_REGION:-us-east-1}"

if [[ "${1:-}" == "--file" ]]; then
  SQL_FILE="$2"
else
  S3_KEY="$1"
  SQL_FILE="/tmp/$(basename "$S3_KEY")"
  aws s3 cp "s3://${ARTIFACT_BUCKET}/${S3_KEY}" "$SQL_FILE" --region "$AWS_REGION"
fi

echo "===== File: $SQL_FILE ====="

# Create session
SESSION=$(curl -sS -X POST "$GATEWAY/v1/sessions" -H 'Content-Type: application/json' -d '{}' | jq -r '.sessionHandle')
if [[ -z "$SESSION" || "$SESSION" == "null" ]]; then
  echo "Failed to create session"
  exit 1
fi
echo "Session: $SESSION"

# Split SQL by ; respecting single-quoted strings (after stripping line comments)
# Encodes each statement as base64 (single line) so bash can mapfile cleanly.
mapfile -t STMTS_B64 < <(python3 - "$SQL_FILE" <<'EOF'
import sys, re, base64
text = open(sys.argv[1]).read()
# strip line comments
text = re.sub(r'--[^\n]*', '', text)
stmts, buf, i, in_quote = [], [], 0, False
while i < len(text):
    c = text[i]
    if c == "'":
        # check for escaped quote ''
        if in_quote and i+1 < len(text) and text[i+1] == "'":
            buf.append("''"); i += 2; continue
        in_quote = not in_quote
        buf.append(c)
    elif c == ';' and not in_quote:
        s = ''.join(buf).strip()
        if s: stmts.append(s)
        buf = []
    else:
        buf.append(c)
    i += 1
tail = ''.join(buf).strip()
if tail: stmts.append(tail)
for s in stmts:
    print(base64.b64encode(s.encode()).decode())
EOF
)
STMTS=()
for b in "${STMTS_B64[@]}"; do
  STMTS+=("$(echo "$b" | base64 -d)")
done

EXIT_CODE=0
for i in "${!STMTS[@]}"; do
  STMT="${STMTS[$i]}"
  echo ""
  echo "===== Statement $((i+1))/${#STMTS[@]} ====="
  echo "  ${STMT:0:200}"
  PAYLOAD=$(jq -nc --arg s "$STMT" '{statement:$s}')
  RESP=$(curl -sS -X POST "$GATEWAY/v1/sessions/$SESSION/statements" -H 'Content-Type: application/json' -d "$PAYLOAD")
  OP=$(echo "$RESP" | jq -r '.operationHandle // empty')
  if [[ -z "$OP" ]]; then
    echo "  ERROR submitting:"
    echo "$RESP" | jq . || echo "$RESP"
    EXIT_CODE=1
    continue
  fi
  echo "  OperationHandle: $OP"

  # Poll for status. Print status every iteration to keep SSM session alive.
  MAX_POLLS="${MAX_POLLS:-180}"
  for j in $(seq 1 $MAX_POLLS); do
    sleep 2
    STATUS=$(curl -sS "$GATEWAY/v1/sessions/$SESSION/operations/$OP/status" | jq -r '.status // "?"')
    echo "  [$j/$MAX_POLLS] $STATUS"
    if [[ "$STATUS" == "FINISHED" || "$STATUS" == "ERROR" || "$STATUS" == "CANCELED" ]]; then break; fi
  done

  # Try to fetch result (for SELECT and INSERT)
  RESULT=$(curl -sS "$GATEWAY/v1/sessions/$SESSION/operations/$OP/result/0" 2>/dev/null)
  if [[ -n "$RESULT" ]]; then
    KIND=$(echo "$RESULT" | jq -r '.resultKind // empty')
    if [[ "$KIND" == "SUCCESS_WITH_CONTENT" ]]; then
      echo "$RESULT" | jq -r '.results.data[]? | .fields | @tsv' | head -20
    fi
    JOBID=$(echo "$RESULT" | jq -r '.jobID // empty')
    [[ -n "$JOBID" ]] && echo "  JobID: $JOBID"
    ERR=$(echo "$RESULT" | jq -r '.errors[]? // empty' 2>/dev/null)
    [[ -n "$ERR" ]] && echo "  Errors: $ERR" && EXIT_CODE=1
  fi
done

curl -sS -X DELETE "$GATEWAY/v1/sessions/$SESSION" >/dev/null
echo ""
echo "===== Done. exit=$EXIT_CODE ====="
exit $EXIT_CODE
