#!/bin/bash
set -e

echo "📊 Creating Superset Charts and Dashboard..."

SUPERSET_URL="http://localhost:8088"
USERNAME="wkdgh"
PASSWORD="357tmdgml!!"

# Login
echo "1️⃣ Logging in..."
LOGIN_RESPONSE=$(curl -s -X POST "${SUPERSET_URL}/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"password\":\"${PASSWORD}\",\"provider\":\"db\",\"refresh\":true}")

ACCESS_TOKEN=$(echo $LOGIN_RESPONSE | python3 -c "import sys, json; print(json.load(sys.stdin).get('access_token', ''))")

if [ -z "$ACCESS_TOKEN" ]; then
    echo "❌ Failed to login"
    exit 1
fi

# Get CSRF Token
CSRF_RESPONSE=$(curl -s -X GET "${SUPERSET_URL}/api/v1/security/csrf_token/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}")

CSRF_TOKEN=$(echo $CSRF_RESPONSE | python3 -c "import sys, json; print(json.load(sys.stdin).get('result', ''))")

echo "✅ Authenticated"

# Get Dataset IDs
echo "2️⃣ Getting dataset IDs..."
DATASETS=$(curl -s -X GET "${SUPERSET_URL}/api/v1/dataset/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}")

PLATFORM_METRICS_ID=$(echo $DATASETS | python3 -c "import sys, json; datasets = json.load(sys.stdin).get('result', []); print(next((str(d['id']) for d in datasets if d.get('table_name') == 'platform_metrics'), ''))")

TRENDING_CONTENT_ID=$(echo $DATASETS | python3 -c "import sys, json; datasets = json.load(sys.stdin).get('result', []); print(next((str(d['id']) for d in datasets if d.get('table_name') == 'trending_content'), ''))")

echo "✅ Platform Metrics Dataset ID: $PLATFORM_METRICS_ID"
echo "✅ Trending Content Dataset ID: $TRENDING_CONTENT_ID"

# Create Chart 1: Platform Views Time Series
echo "3️⃣ Creating Chart 1: Platform Views Over Time..."
CHART1=$(curl -s -X POST "${SUPERSET_URL}/api/v1/chart/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "X-CSRFToken: ${CSRF_TOKEN}" \
  -d "{
    \"slice_name\": \"Platform Views Over Time\",
    \"viz_type\": \"echarts_timeseries_line\",
    \"datasource_id\": ${PLATFORM_METRICS_ID},
    \"datasource_type\": \"table\",
    \"params\": \"{\\\"metrics\\\":[\\\"SUM(total_views)\\\"],\\\"groupby\\\":[\\\"platform\\\"],\\\"time_grain_sqla\\\":\\\"P1H\\\",\\\"time_range\\\":\\\"Last 24 hours\\\"}\",
    \"query_context\": \"{}\"
  }")

CHART1_ID=$(echo $CHART1 | python3 -c "import sys, json; print(json.load(sys.stdin).get('id', ''))" 2>/dev/null || echo "")

if [ ! -z "$CHART1_ID" ]; then
    echo "✅ Chart 1 created (ID: $CHART1_ID)"
else
    echo "⚠️ Chart 1 might already exist or failed to create"
fi

# Create Chart 2: Platform Comparison Bar
echo "4️⃣ Creating Chart 2: Platform Comparison..."
CHART2=$(curl -s -X POST "${SUPERSET_URL}/api/v1/chart/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "X-CSRFToken: ${CSRF_TOKEN}" \
  -d "{
    \"slice_name\": \"Platform Total Views\",
    \"viz_type\": \"echarts_timeseries_bar\",
    \"datasource_id\": ${PLATFORM_METRICS_ID},
    \"datasource_type\": \"table\",
    \"params\": \"{\\\"metrics\\\":[\\\"SUM(total_views)\\\"],\\\"groupby\\\":[\\\"platform\\\"]}\",
    \"query_context\": \"{}\"
  }")

CHART2_ID=$(echo $CHART2 | python3 -c "import sys, json; print(json.load(sys.stdin).get('id', ''))" 2>/dev/null || echo "")

if [ ! -z "$CHART2_ID" ]; then
    echo "✅ Chart 2 created (ID: $CHART2_ID)"
else
    echo "⚠️ Chart 2 might already exist or failed to create"
fi

# Create Chart 3: Trending Content Table
echo "5️⃣ Creating Chart 3: Trending Content Table..."
CHART3=$(curl -s -X POST "${SUPERSET_URL}/api/v1/chart/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "X-CSRFToken: ${CSRF_TOKEN}" \
  -d "{
    \"slice_name\": \"Top Trending Content\",
    \"viz_type\": \"table\",
    \"datasource_id\": ${TRENDING_CONTENT_ID},
    \"datasource_type\": \"table\",
    \"params\": \"{\\\"all_columns\\\":[\\\"platform\\\",\\\"title\\\",\\\"total_views\\\",\\\"buzz_score\\\"],\\\"row_limit\\\":20,\\\"order_desc\\\":true}\",
    \"query_context\": \"{}\"
  }")

CHART3_ID=$(echo $CHART3 | python3 -c "import sys, json; print(json.load(sys.stdin).get('id', ''))" 2>/dev/null || echo "")

if [ ! -z "$CHART3_ID" ]; then
    echo "✅ Chart 3 created (ID: $CHART3_ID)"
else
    echo "⚠️ Chart 3 might already exist or failed to create"
fi

# Create Dashboard
echo "6️⃣ Creating Dashboard..."
DASHBOARD=$(curl -s -X POST "${SUPERSET_URL}/api/v1/dashboard/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "X-CSRFToken: ${CSRF_TOKEN}" \
  -d "{
    \"dashboard_title\": \"Commerce Analytics Dashboard\",
    \"slug\": \"commerce-analytics\",
    \"published\": true
  }")

DASHBOARD_ID=$(echo $DASHBOARD | python3 -c "import sys, json; print(json.load(sys.stdin).get('id', ''))" 2>/dev/null || echo "")

if [ ! -z "$DASHBOARD_ID" ]; then
    echo "✅ Dashboard created (ID: $DASHBOARD_ID)"
else
    echo "⚠️ Dashboard might already exist"
fi

echo ""
echo "✅ Superset Dashboard Setup Complete!"
echo ""
echo "🌐 Access your dashboard at: ${SUPERSET_URL}/dashboard/list/"
echo "👤 Username: ${USERNAME}"
echo "🔑 Password: ${PASSWORD}"
echo ""
echo "📊 Created:"
echo "   - 3 Charts (Platform Views, Platform Comparison, Trending Content)"
echo "   - 1 Dashboard (Commerce Analytics)"
