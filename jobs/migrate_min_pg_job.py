# jobs.py
from dagster import define_asset_job, ScheduleDefinition, AssetSelection
from assets.migrate_min_pg_asset import  stock_alert_job , raw_products_from_minio \
    ,product_bronze , migrate_to_silver_history ,reconcile_inventory_asset

             # Import Asset เข้ามา


alert_job = define_asset_job(
    name="alert_job",
    selection=AssetSelection.assets(stock_alert_job)
)

# --- Define Schedule ---
# ตั้งเวลาให้รันทุกๆ 1 นาที (Cron: */1 * * * *)
alert_schedule = ScheduleDefinition(
    job=alert_job,
    cron_schedule="* * * * *", 
    execution_timezone="Asia/Bangkok"
)



reconcile_job = define_asset_job(
    name="reconcile_job",
    selection=AssetSelection.assets(reconcile_inventory_asset)
)


reconcile_schedule = ScheduleDefinition(
    job=reconcile_job,
    cron_schedule="0 19 * * *", 
    execution_timezone="Asia/Bangkok"
)




product_job = define_asset_job(
    name = "product_job",
    selection = AssetSelection.assets(raw_products_from_minio,product_bronze,migrate_to_silver_history)
)