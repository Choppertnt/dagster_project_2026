# jobs.py
from dagster import define_asset_job, ScheduleDefinition, AssetSelection
from google_health.assets.health_assets import  sync_heart_rate_asset,sync_sleep_asset


heart_rate_job = define_asset_job(
    name="heart_rate_job",
    selection=AssetSelection.assets(sync_heart_rate_asset)
)

# --- Define Schedule ---
heart_rate_schedule = ScheduleDefinition(
    job=heart_rate_job,
    cron_schedule="*/15 * * * *", 
    execution_timezone="Asia/Bangkok"
)

sleep_job = define_asset_job(
    name="sleep_job", selection=AssetSelection.assets(sync_sleep_asset)
)



