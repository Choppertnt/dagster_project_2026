from dagster import Definitions, load_assets_from_modules
from assets import migrate_min_pg_asset
from jobs.migrate_min_pg_job import scd2_job , scd2_schedule , alert_job , alert_schedule
from sensors.failure_alerts import line_oa_failure_sensor

all_assets = load_assets_from_modules([migrate_min_pg_asset])

defs = Definitions(
    assets=all_assets,
    jobs=[scd2_job,alert_job],
    schedules = [scd2_schedule,alert_schedule],
    sensors=[line_oa_failure_sensor]
) 