from dagster import Definitions, load_assets_from_modules
from google_health.assets import health_assets

from dagster_aws.s3 import s3_pickle_io_manager, s3_resource
from google_health.jobs.health_job import heart_rate_job,heart_rate_schedule,sleep_job
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
    # "endpoint_url": "http://localhost:9000", 
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
    "use_ssl": False,
})


all_assets = load_assets_from_modules([health_assets])

defs = Definitions(
    assets=all_assets,
    jobs = [heart_rate_job,sleep_job],
    schedules = [heart_rate_schedule],
    resources={
            "io_manager": minio_io_manager,
            "s3": minio_resource,
        },
) 