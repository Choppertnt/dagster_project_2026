from dagster import asset, AssetExecutionContext , AssetIn
import pandas as pd
from minio import Minio 
import io
import os
from supabase import create_client, Client
from fastembed import TextEmbedding
from datetime import datetime
import urllib.parse
import psycopg
# ข้อมูลการเชื่อมต่อ (แนะนำให้ใช้ Environment Variables เพื่อความปลอดภัยครับ)
MINIO_ENDPOINT = "minio-api-route-thanathorn55551-dev.apps.rm2.thpm.p1.openshiftapps.com"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")



from dagster import asset, AssetExecutionContext

# --- ส่วนการตั้งค่า Connection ---
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

encoded_pass = urllib.parse.quote_plus(DB_PASS) if DB_PASS else ""
CONN_STR = f"postgresql://{DB_USER}:{encoded_pass}@{DB_HOST}:5432/{DB_NAME}"

embedding_model = TextEmbedding("sentence-transformers/all-MiniLM-L6-v2")
@asset
def raw_products_from_minio(context: AssetExecutionContext):
    # 1. สร้างการเชื่อมต่อกับ MinIO Client
    context.log.info(f"กำลังเชื่อมต่อกับ MinIO ที่: {MINIO_ENDPOINT}")
    
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # OpenShift route ส่วนใหญ่เป็น HTTPS
    )

    bucket_name = "external-csv" # เปลี่ยนเป็นชื่อ bucket ของพี่
    object_name = "stg_products.csv"    # เปลี่ยนเป็นชื่อไฟล์ CSV ของพี่

    try:
        # 2. ดึงข้อมูลออกมาเป็น Stream
        context.log.info(f"กำลังดึงไฟล์ {object_name} จาก Bucket {bucket_name}...")
        response = client.get_object(bucket_name, object_name)
        
        # 3. ใช้ Pandas อ่านข้อมูลจาก Stream โดยตรง
        # เราใช้ BytesIO เพื่อแปลงข้อมูลจาก MinIO ให้ Pandas อ่านได้เหมือนไฟล์ปกติครับ
        df = pd.read_csv(io.BytesIO(response.read()))
        
        context.log.info(f"ดึงข้อมูลสำเร็จ! พบข้อมูลทั้งหมด {len(df)} รายการ")
        context.log.info(f"คอลัมน์ที่พบ: {df.columns.tolist()}")

        return df

    except Exception as e:
        context.log.error(f"เกิดข้อผิดพลาดในการดึงข้อมูล: {str(e)}")
        raise e
    finally:
        # ปิดการเชื่อมต่อ
        if 'response' in locals():
            response.close()
            response.release_conn()


@asset
def raw_products_from_minio2(context: AssetExecutionContext):
    # 1. สร้างการเชื่อมต่อกับ MinIO Client
    context.log.info(f"กำลังเชื่อมต่อกับ MinIO ที่: {MINIO_ENDPOINT}")
    
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # OpenShift route ส่วนใหญ่เป็น HTTPS
    )

    bucket_name = "external-csv" # เปลี่ยนเป็นชื่อ bucket ของพี่
    object_name = "stg_inventory.csv"    # เปลี่ยนเป็นชื่อไฟล์ CSV ของพี่

    try:
        # 2. ดึงข้อมูลออกมาเป็น Stream
        context.log.info(f"กำลังดึงไฟล์ {object_name} จาก Bucket {bucket_name}...")
        response = client.get_object(bucket_name, object_name)
        
        # 3. ใช้ Pandas อ่านข้อมูลจาก Stream โดยตรง
        # เราใช้ BytesIO เพื่อแปลงข้อมูลจาก MinIO ให้ Pandas อ่านได้เหมือนไฟล์ปกติครับ
        df = pd.read_csv(io.BytesIO(response.read()))
        
        context.log.info(f"ดึงข้อมูลสำเร็จ! พบข้อมูลทั้งหมด {len(df)} รายการ")
        context.log.info(f"คอลัมน์ที่พบ: {df.columns.tolist()}")

        return df

    except Exception as e:
        context.log.error(f"เกิดข้อผิดพลาดในการดึงข้อมูล: {str(e)}")
        raise e
    finally:
        # ปิดการเชื่อมต่อ
        if 'response' in locals():
            response.close()
            response.release_conn()

@asset(deps=['raw_products_from_minio'])
def migrate_to_bronze_tables(context: AssetExecutionContext , raw_products_from_minio):
    df = raw_products_from_minio
    
    # 1. เตรียมข้อมูลเป็น List of Tuples สำหรับ psycopg
    # ตรวจสอบให้แน่ใจว่าชื่อ Column ใน DataFrame ตรงกับใน Postgres
    records = [tuple(x) for x in df.values]
    columns = ", ".join(df.columns)
    
    # สร้าง Placeholder (%s, %s, ...) ตามจำนวน Column
    placeholders = ", ".join(["%s"] * len(df.columns))
    
    # 2. สร้างคำสั่ง SQL Upsert (ON CONFLICT)
    # สมมติว่าต้องการให้ถ้าซ้ำแล้วทำการ Update ข้อมูลเดิม (เหมือน upsert ของ Supabase)
    update_statement = ", ".join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in ['product_id', 'update_at']])
    
    query = f"""
        INSERT INTO stg_products ({columns})
        VALUES ({placeholders})
        ON CONFLICT (product_id, update_at) 
        DO UPDATE SET {update_statement};
    """

    context.log.info(f"กำลังส่งข้อมูล {len(df)} แถว ไปยัง PostgreSQL...")

    try:
        # 3. เชื่อมต่อและรันคำสั่ง
        with psycopg.connect(CONN_STR) as conn:
            with conn.cursor() as cur:
                # ใช้ executemany เพื่อประสิทธิภาพในการส่งข้อมูลปริมาณมาก
                cur.executemany(query, records)
            conn.commit()
            
        context.log.info(f"✅ Ingest ข้อมูลสำเร็จ! บันทึกลงตาราง stg_products เรียบร้อยแล้ว")

    except Exception as e:
        context.log.error(f"❌ เกิดข้อผิดพลาดในการเชื่อมต่อ PostgreSQL: {str(e)}")
        raise e
        
    return df


@asset(deps=['raw_products_from_minio2'])
def migrate_to_bronze_tables2(context: AssetExecutionContext , raw_products_from_minio2):
    df = raw_products_from_minio2
    
    # 1. เตรียมข้อมูลเป็น List of Tuples สำหรับ psycopg
    # ตรวจสอบให้แน่ใจว่าชื่อ Column ใน DataFrame ตรงกับใน Postgres
    records = [tuple(x) for x in df.values]
    columns = ", ".join(df.columns)
    
    # สร้าง Placeholder (%s, %s, ...) ตามจำนวน Column
    placeholders = ", ".join(["%s"] * len(df.columns))
    
    # 2. สร้างคำสั่ง SQL Upsert (ON CONFLICT)
    # สมมติว่าต้องการให้ถ้าซ้ำแล้วทำการ Update ข้อมูลเดิม (เหมือน upsert ของ Supabase)
    update_statement = ", ".join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in ['product_id', 'update_at']])
    
    query = f"""
        INSERT INTO stg_inventory ({columns})
        VALUES ({placeholders})
        ON CONFLICT (product_id,warehouse_id, last_stock_check) 
        DO UPDATE SET {update_statement};
    """

    context.log.info(f"กำลังส่งข้อมูล {len(df)} แถว ไปยัง PostgreSQL...")

    try:
        # 3. เชื่อมต่อและรันคำสั่ง
        with psycopg.connect(CONN_STR) as conn:
            with conn.cursor() as cur:
                # ใช้ executemany เพื่อประสิทธิภาพในการส่งข้อมูลปริมาณมาก
                cur.executemany(query, records)
            conn.commit()
            
        context.log.info(f"✅ Ingest ข้อมูลสำเร็จ! บันทึกลงตาราง stg_products เรียบร้อยแล้ว")

    except Exception as e:
        context.log.error(f"❌ เกิดข้อผิดพลาดในการเชื่อมต่อ PostgreSQL: {str(e)}")
        raise e
        
    return df

@asset(deps=['migrate_to_bronze_tables'])
def migrate_to_silver_history(context: AssetExecutionContext, migrate_to_bronze_tables):
    """
    ขั้นตอน Silver: ทำ Vector Search และเก็บประวัติแบบ SCD Type 2 ลง PostgreSQL
    """
    df_bronze = migrate_to_bronze_tables
    now = datetime.utcnow()

    if df_bronze.empty:
        context.log.warning("ไม่มีข้อมูลใหม่จาก Bronze Layer")
        return pd.DataFrame()

    new_records_to_insert = []

    try:
        with psycopg.connect(CONN_STR) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                # 1. ดึงข้อมูล Active ปัจจุบันจาก Postgres มาเปรียบเทียบ
                cur.execute("SELECT * FROM dim_products_history WHERE is_current = True")
                current_silver_data = cur.fetchall()
                df_current = pd.DataFrame(current_silver_data)

                for _, row in df_bronze.iterrows():
                    pid = str(row['product_id'])
                    
                    # ตรวจสอบว่ามีสินค้านี้ในระบบหรือยัง
                    match = df_current[df_current['product_id'] == pid] if not df_current.empty else pd.DataFrame()
                    
                    has_changed = False
                    if not match.empty:
                        current_row = match.iloc[0]
                        # เช็คการเปลี่ยนแปลง (ราคา หรือ ชื่อ)
                        if (float(row['base_price']) != float(current_row['base_price']) or 
                            str(row['product_name']) != str(current_row['product_name'])):
                            has_changed = True
                            
                            # A. สั่งปิด Record เก่า (Expire)
                            context.log.info(f"สินค้า {pid} เปลี่ยนข้อมูล -> กำลัง Expire Record เก่า")
                            cur.execute(
                                """
                                UPDATE dim_products_history 
                                SET is_current = False, end_date = %s 
                                WHERE product_id = %s AND is_current = True
                                """,
                                (now, pid)
                            )

                    # กรณีเป็นสินค้าใหม่ หรือ สินค้าเดิมที่ข้อมูลเปลี่ยน
                    if match.empty or has_changed:
                        context.log.info(f"กำลังทำ Embedding สำหรับ: {row['product_name']}")
                        
                        # B. สร้าง Vector ด้วย FastEmbed
                        rich_description = f"Brand: {row['brand']} | Category: {row['category']} | Product: {row['product_name']}"
                        embeddings = list(embedding_model.embed([rich_description]))
                        product_vector = embeddings[0].tolist()
                        
                        # C. เตรียมข้อมูลสำหรับ Insert ใหม่ (ใช้ Tuple เพื่อส่งเข้า Postgres)
                        new_records_to_insert.append((
                            pid,
                            row['product_name'],
                            row['category'],
                            row['brand'],
                            int(row['base_price']),
                            now,
                            None,
                            True,
                            product_vector  # psycopg จะแปลง list เป็น vector format ให้เอง
                        ))

                # 2. บันทึกข้อมูลใหม่ทั้งหมดลง Silver Table (Batch Insert)
                if new_records_to_insert:
                    cur.executemany(
                        """
                        INSERT INTO dim_products_history 
                        (product_id, product_name, category, brand, base_price, start_date, end_date, is_current, product_vector)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        new_records_to_insert
                    )
                    conn.commit()
                    context.log.info(f"✅ บันทึกข้อมูลลง Silver สำเร็จ {len(new_records_to_insert)} รายการ")
                else:
                    context.log.info("ไม่มีการเปลี่ยนแปลงข้อมูล ไม่ต้องอัปเดต Silver")

    except Exception as e:
        context.log.error(f"❌ เกิดข้อผิดพลาดในขั้นตอน Silver: {str(e)}")
        raise e

    # ส่งค่ากลับเป็น DataFrame เพื่อใช้ใน Asset ถัดไป (ถ้ามี)
    return pd.DataFrame(new_records_to_insert, columns=[
        "product_id", "product_name", "category", "brand", "base_price", 
        "start_date", "end_date", "is_current", "product_vector"
    ])
