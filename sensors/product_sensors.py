import os
from minio import Minio
from minio.error import S3Error
from dagster import sensor, RunRequest
from jobs.migrate_min_pg_job import product_job

@sensor(job=product_job)
def minio_product_csv_sensor(context):
    
    # 1. เชื่อมต่อ MinIO (แบบเดียวกับที่คุณใช้ใน Asset เลย)
    # ⚠️ ข้อควรระวัง: ห้ามใส่ http:// หรือ https:// นำหน้า
    client = Minio(
        "minio-api-route-thanathorn55551-dev.apps.rm2.thpm.p1.openshiftapps.com", 
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False  # ถ้า Route บน OpenShift เป็น HTTPS ให้เปลี่ยนเป็น True ครับ
    )
    
    bucket_name = "external-csv"
    target_file = "stg_products.csv"
    
    try:
        # 2. เช็คข้อมูลไฟล์ (Stat) ตรงๆ เลย
        stat = client.stat_object(bucket_name, target_file)
        last_modified = stat.last_modified.isoformat()
        
        # 3. สร้าง Run Key จากเวลาที่ไฟล์ถูกแก้ล่าสุด
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
        
    except S3Error as err:
        if err.code == "NoSuchKey":
            # กรณีที่ยังไม่มีไฟล์อัปโหลดเข้ามา (ไม่ต้องทำอะไร แค่รอต่อไป)
            pass
        else:
            context.log.error(f"❌ MinIO Error: {err}")
    except Exception as e:
        context.log.error(f"❌ Unexpected Error checking MinIO: {e}")