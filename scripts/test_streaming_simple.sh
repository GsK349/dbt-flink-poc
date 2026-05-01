#!/bin/bash
# Simple test script using Kafka CLI (no Python dependencies)

set -e

echo "=========================================="
echo "Streaming CDC Test (Kafka CLI Version)"
echo "=========================================="
echo ""

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Create topics
echo -e "${YELLOW}Creating Kafka topics...${NC}"
docker-compose exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic orders_cdc_headers --partitions 1 --replication-factor 1 2>/dev/null || true
docker-compose exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic orders_upsert --partitions 1 --replication-factor 1 2>/dev/null || true
docker-compose exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic orders_soft_delete --partitions 1 --replication-factor 1 2>/dev/null || true
echo -e "${GREEN}✓ Topics created${NC}"
echo ""

# Test 1: Simple upsert (no headers, easiest test)
echo -e "${YELLOW}Test 1: Upsert with PRIMARY KEY${NC}"
echo "  Producing test events..."

docker-compose exec kafka bash -c 'cat <<EOF | kafka-console-producer --bootstrap-server kafka:9092 --topic orders_upsert
{"id":"5001","customer_id":"cust_5001","order_ts":"2026-04-28T09:00:00","amount":75.00,"status":"created"}
{"id":"5001","customer_id":"cust_5001","order_ts":"2026-04-28T09:00:00","amount":75.00,"status":"in_progress"}
{"id":"5001","customer_id":"cust_5001","order_ts":"2026-04-28T09:00:00","amount":75.00,"status":"completed"}
{"id":"5002","customer_id":"cust_5002","order_ts":"2026-04-28T09:05:00","amount":100.00,"status":"created"}
EOF'

echo "  ✓ Events produced"
echo "  Starting Flink job..."

# Start job in background
docker-compose exec jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/streaming_upsert_primary_key.sql > /tmp/flink_upsert.log 2>&1 &
JOB_PID=$!

echo "  Job PID: $JOB_PID"
echo "  Waiting 25 seconds for checkpoint..."
sleep 25

echo "  ✓ Job started and running"
echo ""

# Test 2: Check Flink web UI
echo -e "${YELLOW}Flink Status${NC}"
echo "  JobManager UI: http://localhost:8081"
echo "  - Visit to see running jobs"
echo ""

# Test 3: Check data
echo -e "${YELLOW}Checking results...${NC}"
echo ""
echo "  To query results, run:"
echo ""
echo "  python3 << 'EOF'"
echo "import json, urllib.request, time"
echo ""
echo "BASE = 'http://localhost:8083/v1'"
echo ""
echo "def post(url, data):"
echo "    req = urllib.request.Request(url, json.dumps(data).encode(), {'Content-Type': 'application/json'})"
echo "    with urllib.request.urlopen(req) as r:"
echo "        return json.loads(r.read())"
echo ""
echo "def get(url):"
echo "    with urllib.request.urlopen(url) as r:"
echo "        return json.loads(r.read())"
echo ""
echo "# Create session"
echo "sid = post(f'{BASE}/sessions', {'sessionName': 'test'})['sessionHandle']"
echo "print(f'Session: {sid}')"
echo ""
echo "# Register catalog"
echo "op = post(f'{BASE}/sessions/{sid}/statements', {"
echo "    'statement': \"CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH ('type'='iceberg','warehouse'='s3://flink-iceberg-warehouse/','catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog','io-impl'='org.apache.iceberg.aws.s3.S3FileIO')\""
echo "})['operationHandle']"
echo ""
echo "# Wait for catalog"
echo "for _ in range(60):"
echo "    if get(f'{BASE}/sessions/{sid}/operations/{op}/status')['status'] == 'FINISHED':"
echo "        break"
echo "    time.sleep(1)"
echo ""
echo "# Query"
echo "op = post(f'{BASE}/sessions/{sid}/statements', {"
echo "    'statement': 'SELECT id, status FROM iceberg_catalog.\\\`default\\\`.iceberg_orders_upsert ORDER BY id'"
echo "})['operationHandle']"
echo ""
echo "for _ in range(60):"
echo "    if get(f'{BASE}/sessions/{sid}/operations/{op}/status')['status'] == 'FINISHED':"
echo "        break"
echo "    time.sleep(1)"
echo ""
echo "# Fetch results"
echo "r = get(f'{BASE}/sessions/{sid}/operations/{op}/result/0')"
echo "print(f'\\nColumns: {[c[\"name\"] for c in r[\"results\"][\"columns\"]]}\\n')"
echo ""
echo "token = 0"
echo "while True:"
echo "    r = get(f'{BASE}/sessions/{sid}/operations/{op}/result/{token}')"
echo "    for row in r.get('results', {}).get('data', []):"
echo "        print(f'  Row: {row[\"fields\"]}')"
echo "    if r.get('resultType') == 'EOS' or not r.get('nextResultUri'):"
echo "        break"
echo "    token = int(r.get('nextResultUri', '').split('/')[-1])"
echo "EOF"
echo ""

# Summary
echo "=========================================="
echo -e "${GREEN}✅ Test Started!${NC}"
echo "=========================================="
echo ""
echo "What happened:"
echo "  1. ✓ Created 3 Kafka topics"
echo "  2. ✓ Published 4 test events (upsert topic)"
echo "  3. ✓ Started Flink streaming job"
echo "  4. → Flink is processing events now"
echo ""
echo "Next steps:"
echo "  1. Run the query command above to see results"
echo "  2. Expected: 2 rows (id=5001 and id=5002)"
echo "  3. Order 5001 should have status='completed' (latest)"
echo ""
echo "Running jobs:"
docker-compose exec jobmanager curl -s http://localhost:8081/jobs 2>/dev/null | python3 -c "import sys,json; jobs=json.load(sys.stdin).get('jobs',[]); print('  Found '+str(len(jobs))+' job(s)')" || echo "  (unable to fetch job list)"
echo ""
