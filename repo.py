from dagster import Definitions, load_assets_from_modules
from assets import migrate_min_pg_asset
from jobs.migrate_min_pg_job import  alert_job , alert_schedule , product_job , reconcile_job , reconcile_schedule
from sensors.failure_alerts import line_oa_failure_sensor 
from sensors.new_user import stg_userprofile_sensor
from sensors.product_sensors import minio_product_csv_sensor
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource
import os

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
minio_io_manager = s3_pickle_io_manager.configured({
    "s3_bucket": "dagster-assets",    # นายต้องสร้าง Bucket นี้ใน MinIO ก่อนนะ
    "s3_prefix": "prod",
})

# 2. นิยาม S3 Resource เพื่อต่อท่อ
minio_resource = s3_resource.configured({
    "endpoint_url": "http://minio.minio.svc.cluster.local:9000", 
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
    "use_ssl": False,
})


all_assets = load_assets_from_modules([migrate_min_pg_asset])

defs = Definitions(
    assets=all_assets,
    jobs=[alert_job,product_job,reconcile_job],
    resources={
            "io_manager": minio_io_manager,
            "s3": minio_resource,
        },
    schedules = [alert_schedule,reconcile_schedule],
    sensors=[line_oa_failure_sensor , stg_userprofile_sensor,minio_product_csv_sensor]
) 