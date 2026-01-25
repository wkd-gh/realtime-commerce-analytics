#!/usr/bin/env python3
"""
Superset setup script - runs inside container
"""
import os
from superset import db
from superset.models.core import Database
from superset.connectors.sqla.models import SqlaTable
from superset.models.slice import Slice
from superset.models.dashboard import Dashboard

print("🚀 Setting up Superset Commerce Analytics...")

# Step 1: Create Database Connection
print("1️⃣ Creating database connection...")
database = Database(
    database_name="Commerce Analytics",
    sqlalchemy_uri="postgresql://wkdgh:357tmdgml!!@postgres:5432/commerce_analytics",
    expose_in_sqllab=True,
    allow_ctas=True,
    allow_cvas=True,
    allow_dml=True
)

# Check if database already exists
existing_db = db.session.query(Database).filter_by(database_name="Commerce Analytics").first()
if existing_db:
    print(f"✅ Database connection already exists (ID: {existing_db.id})")
    database = existing_db
else:
    db.session.add(database)
    db.session.commit()
    print(f"✅ Database connection created (ID: {database.id})")

# Step 2: Create Datasets
print("2️⃣ Creating datasets...")

# Platform Metrics Dataset
platform_metrics_table = db.session.query(SqlaTable).filter_by(
    table_name="platform_metrics",
    database_id=database.id
).first()

if not platform_metrics_table:
    platform_metrics_table = SqlaTable(
        table_name="platform_metrics",
        schema="public",
        database=database,
        owners=[]
    )
    db.session.add(platform_metrics_table)
    db.session.commit()
    print("✅ Created platform_metrics dataset")
else:
    print("✅ platform_metrics dataset already exists")

# Trending Content Dataset
trending_table = db.session.query(SqlaTable).filter_by(
    table_name="trending_content",
    database_id=database.id
).first()

if not trending_table:
    trending_table = SqlaTable(
        table_name="trending_content",
        schema="public",
        database=database,
        owners=[]
    )
    db.session.add(trending_table)
    db.session.commit()
    print("✅ Created trending_content dataset")
else:
    print("✅ trending_content dataset already exists")

# Step 3: Create Charts
print("3️⃣ Creating charts...")

charts_config = [
    {
        "slice_name": "Platform Views Over Time",
        "viz_type": "echarts_timeseries_line",
        "datasource_id": platform_metrics_table.id,
        "datasource_type": "table",
        "params": {
            "metrics": ["SUM(total_views)"],
            "groupby": ["platform"],
            "time_grain_sqla": "P1D",
            "time_range": "Last 7 days",
        }
    },
    {
        "slice_name": "Platform Comparison",
        "viz_type": "echarts_timeseries_bar",
        "datasource_id": platform_metrics_table.id,
        "datasource_type": "table",
        "params": {
            "metrics": ["SUM(total_views)"],
            "groupby": ["platform"],
        }
    },
    {
        "slice_name": "Trending Content Table",
        "viz_type": "table",
        "datasource_id": trending_table.id,
        "datasource_type": "table",
        "params": {
            "all_columns": ["platform", "title", "total_views", "buzz_score"],
            "row_limit": 20,
        }
    }
]

chart_ids = []
for chart_config in charts_config:
    existing_chart = db.session.query(Slice).filter_by(
        slice_name=chart_config["slice_name"]
    ).first()

    if not existing_chart:
        chart = Slice(
            slice_name=chart_config["slice_name"],
            viz_type=chart_config["viz_type"],
            datasource_id=chart_config["datasource_id"],
            datasource_type=chart_config["datasource_type"],
            params=str(chart_config["params"]),
            owners=[]
        )
        db.session.add(chart)
        db.session.commit()
        chart_ids.append(chart.id)
        print(f"✅ Created chart: {chart_config['slice_name']}")
    else:
        chart_ids.append(existing_chart.id)
        print(f"✅ Chart already exists: {chart_config['slice_name']}")

# Step 4: Create Dashboard
print("4️⃣ Creating dashboard...")

dashboard = db.session.query(Dashboard).filter_by(
    dashboard_title="Commerce Analytics Dashboard"
).first()

if not dashboard:
    dashboard = Dashboard(
        dashboard_title="Commerce Analytics Dashboard",
        position_json="{}",
        slices=[],
        owners=[]
    )
    db.session.add(dashboard)
    db.session.commit()
    print("✅ Dashboard created")
else:
    print("✅ Dashboard already exists")

# Add charts to dashboard
charts = db.session.query(Slice).filter(Slice.id.in_(chart_ids)).all()
for chart in charts:
    if chart not in dashboard.slices:
        dashboard.slices.append(chart)

db.session.commit()

print("\n✅ Superset setup complete!")
print(f"\n🌐 Access Superset at: http://localhost:8088")
print(f"👤 Username: wkdgh")
print(f"🔑 Password: 357tmdgml!!")
print(f"\n📊 Dashboard: Commerce Analytics Dashboard")
print(f"📈 Charts created: {len(chart_ids)}")
