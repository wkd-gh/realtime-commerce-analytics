#!/usr/bin/env python3
"""
Setup Superset with database connection and sample charts
"""
import requests
import json
import time

SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"

print("🚀 Setting up Superset...")

# Step 1: Login
print("1️⃣ Logging in to Superset...")
session = requests.Session()

# Get CSRF token
response = session.get(f"{SUPERSET_URL}/login/")
if response.status_code != 200:
    print(f"❌ Failed to access Superset: {response.status_code}")
    exit(1)

# Login
login_data = {
    "username": USERNAME,
    "password": PASSWORD,
    "provider": "db"
}

response = session.post(
    f"{SUPERSET_URL}/api/v1/security/login",
    json=login_data
)

if response.status_code == 200:
    login_result = response.json()
    access_token = login_result.get("access_token")

    if access_token:
        session.headers.update({
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        })
        print("✅ Logged in successfully")
    else:
        print("❌ No access token received")
        exit(1)
else:
    print(f"❌ Login failed: {response.status_code}")
    print(response.text)
    exit(1)

# Step 2: Get CSRF token
print("2️⃣ Getting CSRF token...")
response = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
if response.status_code == 200:
    csrf_token = response.json().get("result")
    session.headers.update({
        "X-CSRFToken": csrf_token,
        "Referer": SUPERSET_URL
    })
    print("✅ CSRF token obtained")
else:
    print(f"⚠️ Could not get CSRF token: {response.status_code}")

# Step 3: Create database connection
print("3️⃣ Creating PostgreSQL database connection...")
db_data = {
    "database_name": "Commerce Analytics",
    "sqlalchemy_uri": "postgresql://wkdgh:357tmdgml!!@postgres:5432/commerce_analytics",
    "expose_in_sqllab": True,
    "allow_ctas": True,
    "allow_cvas": True,
    "allow_dml": True
}

response = session.post(
    f"{SUPERSET_URL}/api/v1/database/",
    json=db_data
)

if response.status_code in [200, 201]:
    db_result = response.json()
    db_id = db_result.get("id")
    print(f"✅ Database connection created (ID: {db_id})")
elif response.status_code == 422:
    # Database might already exist
    print("⚠️ Database connection might already exist")
    # Try to get existing database
    response = session.get(f"{SUPERSET_URL}/api/v1/database/")
    if response.status_code == 200:
        databases = response.json().get("result", [])
        for db in databases:
            if "commerce" in db.get("database_name", "").lower():
                db_id = db.get("id")
                print(f"✅ Using existing database (ID: {db_id})")
                break
else:
    print(f"❌ Failed to create database: {response.status_code}")
    print(response.text)
    db_id = None

# Step 4: Create datasets
print("4️⃣ Creating datasets...")

datasets_to_create = [
    {
        "table_name": "platform_metrics",
        "schema": "public",
        "database": db_id
    },
    {
        "table_name": "trending_content",
        "schema": "public",
        "database": db_id
    }
]

dataset_ids = {}
for dataset_info in datasets_to_create:
    if db_id:
        response = session.post(
            f"{SUPERSET_URL}/api/v1/dataset/",
            json=dataset_info
        )

        if response.status_code in [200, 201]:
            dataset_result = response.json()
            dataset_id = dataset_result.get("id")
            dataset_ids[dataset_info["table_name"]] = dataset_id
            print(f"✅ Dataset '{dataset_info['table_name']}' created (ID: {dataset_id})")
        elif response.status_code == 422:
            print(f"⚠️ Dataset '{dataset_info['table_name']}' might already exist")
        else:
            print(f"❌ Failed to create dataset '{dataset_info['table_name']}': {response.status_code}")

print("\n📊 Superset Setup Complete!")
print(f"\n🌐 Access Superset at: {SUPERSET_URL}")
print(f"👤 Username: {USERNAME}")
print(f"🔑 Password: {PASSWORD}")
print("\n📝 Next steps:")
print("   1. Go to SQL Lab to run queries")
print("   2. Create charts from the datasets")
print("   3. Build dashboards with your charts")
