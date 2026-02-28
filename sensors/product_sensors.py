import os
import boto3
from dagster import sensor, RunRequest
from jobs.migrate_min_pg_job import product_job

@sensor(job=product_job) # ตรวจสอบว่าชื่อ job ตรงกับที่นิยามไว้ใน repo.py
def minio_product_csv_sensor(context):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio-api-route-thanathorn55551-dev.apps.rm2.thpm.p1.openshiftapps.com",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    )
    
    bucket_name = "external-csv"
    target_file = "raw/stg_products.csv" # ปรับเป็น "stg_products.csv" หากไฟล์ไม่ได้อยู่ในโฟลเดอร์ raw/
    
    try:
        # ตรวจสอบไฟล์เป้าหมายใน MinIO
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=target_file)
        
        if "Contents" in response:
            for obj in response["Contents"]:
                if obj["Key"] == target_file:
                    last_modified = obj["LastModified"].isoformat()
                    # ใช้ชื่อไฟล์และเวลาแก้ไขล่าสุดเป็น run_key เพื่อให้ Sensor รันใหม่เมื่อไฟล์ถูกอัปเดตทับ
                    run_key = f"product_{target_file.replace('/', '_')}_{last_modified}"
                    
                    yield RunRequest(
                        run_key=run_key,
                        run_config={
                            "ops": {
                                "raw_products_from_minio": {
                                    "config": {"s3_key": target_file}
                                }
                            }
                        }
                    )
    except Exception as e:
        context.log.error(f"Error checking MinIO: {e}")