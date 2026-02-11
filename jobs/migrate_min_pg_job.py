# jobs.py
from dagster import define_asset_job, ScheduleDefinition, AssetSelection
from assets.migrate_min_pg_asset import user_profile_silver , stock_alert_job # Import Asset เข้ามา

# --- Define Job ---
# สร้าง Job ชื่อ "process_scd2_job" ที่เลือกเฉพาะ asset "user_profile_silver"
scd2_job = define_asset_job(
    name="process_scd2_job",
    selection=AssetSelection.assets(user_profile_silver)
)

# --- Define Schedule ---
# ตั้งเวลาให้รันทุกๆ 1 นาที (Cron: */1 * * * *)
scd2_schedule = ScheduleDefinition(
    job=scd2_job,
    cron_schedule="0 12 * * *", 
    execution_timezone="Asia/Bangkok"
)


alert_job = define_asset_job(
    name="alert_job",
    selection=AssetSelection.assets(stock_alert_job)
)

# --- Define Schedule ---
# ตั้งเวลาให้รันทุกๆ 1 นาที (Cron: */1 * * * *)
alert_schedule = ScheduleDefinition(
    job=alert_job,
    cron_schedule="1 * * * *", 
    execution_timezone="Asia/Bangkok"
)