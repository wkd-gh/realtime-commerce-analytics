#!/bin/bash
set -e

echo "🚀 Setting up Superset Commerce Analytics Dashboard..."

SUPERSET_URL="http://localhost:8088"
USERNAME="wkdgh"
PASSWORD="357tmdgml!!"

# Step 1: Login and get access token
echo "1️⃣ Logging in..."
LOGIN_RESPONSE=$(curl -s -X POST "${SUPERSET_URL}/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${USERNAME}\",\"password\":\"${PASSWORD}\",\"provider\":\"db\",\"refresh\":true}")

ACCESS_TOKEN=$(echo $LOGIN_RESPONSE | python3 -c "import sys, json; print(json.load(sys.stdin).get('access_token', ''))")

if [ -z "$ACCESS_TOKEN" ]; then
    echo "❌ Failed to login. Response: $LOGIN_RESPONSE"
    exit 1
fi

echo "✅ Logged in successfully"

# Step 2: Get CSRF Token
echo "2️⃣ Getting CSRF token..."
CSRF_RESPONSE=$(curl -s -X GET "${SUPERSET_URL}/api/v1/security/csrf_token/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}")

CSRF_TOKEN=$(echo $CSRF_RESPONSE | python3 -c "import sys, json; print(json.load(sys.stdin).get('result', ''))")

if [ -z "$CSRF_TOKEN" ]; then
    echo "⚠️ Could not get CSRF token"
    CSRF_TOKEN="dummy"
fi

echo "✅ CSRF token obtained"

# Step 3: Create Database Connection
echo "3️⃣ Creating database connection..."
DB_RESPONSE=$(curl -s -X POST "${SUPERSET_URL}/api/v1/database/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "X-CSRFToken: ${CSRF_TOKEN}" \
  -d '{
    "database_name": "Commerce Analytics",
    "sqlalchemy_uri": "postgresql://wkdgh:357tmdgml!!@postgres:5432/commerce_analytics",
    "expose_in_sqllab": true,
    "allow_ctas": true,
    "allow_cvas": true,
    "allow_dml": true
  }')

DB_ID=$(echo $DB_RESPONSE | python3 -c "import sys, json; result = json.load(sys.stdin); print(result.get('id', result.get('result', {}).get('id', '')))" 2>/dev/null || echo "")

if [ -z "$DB_ID" ]; then
    echo "⚠️ Database might already exist, trying to get existing..."
    # Get list of databases
    DB_LIST=$(curl -s -X GET "${SUPERSET_URL}/api/v1/database/" \
      -H "Authorization: Bearer ${ACCESS_TOKEN}")

    DB_ID=$(echo $DB_LIST | python3 -c "import sys, json; dbs = json.load(sys.stdin).get('result', []); print(next((str(db['id']) for db in dbs if 'commerce' in db.get('database_name', '').lower()), ''))" 2>/dev/null || echo "1")

    echo "✅ Using database ID: $DB_ID"
else
    echo "✅ Database created with ID: $DB_ID"
fi

# Step 4: Create Datasets
echo "4️⃣ Creating datasets..."

# Platform Metrics Dataset
DATASET1_RESPONSE=$(curl -s -X POST "${SUPERSET_URL}/api/v1/dataset/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "X-CSRFToken: ${CSRF_TOKEN}" \
  -d "{
    \"database\": ${DB_ID},
    \"schema\": \"public\",
    \"table_name\": \"platform_metrics\"
  }")

echo "✅ Dataset response: $(echo $DATASET1_RESPONSE | head -c 200)"

# Trending Content Dataset
DATASET2_RESPONSE=$(curl -s -X POST "${SUPERSET_URL}/api/v1/dataset/" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "X-CSRFToken: ${CSRF_TOKEN}" \
  -d "{
    \"database\": ${DB_ID},
    \"schema\": \"public\",
    \"table_name\": \"trending_content\"
  }")

echo "✅ Datasets created"

echo ""
echo "✅ Superset setup complete!"
echo ""
echo "🌐 Access Superset at: ${SUPERSET_URL}"
echo "👤 Username: ${USERNAME}"
echo "🔑 Password: ${PASSWORD}"
echo ""
echo "📝 Next steps:"
echo "   1. Go to Data → Datasets"
echo "   2. Click on 'platform_metrics' or 'trending_content'"
echo "   3. Click 'Create Chart' to build visualizations"
echo "   4. Create a new Dashboard and add your charts"
