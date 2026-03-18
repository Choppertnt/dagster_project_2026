import psycopg
import urllib.parse
from dagster import sensor, RunRequest, define_asset_job, AssetSelection

import os
user_profile_silver_job = define_asset_job("user_profile_silver_job", AssetSelection.assets("user_profile_silver"))
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

encoded_pass = urllib.parse.quote_plus(DB_PASS) if DB_PASS else ""
CONN_STR = f"postgresql://{DB_USER}:{encoded_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


@sensor(job=user_profile_silver_job)
def stg_userprofile_sensor(context):
    # 1. อ่านเวลาที่เคยรันล่าสุดจาก Cursor (ถ้ารันครั้งแรกให้เป็นอดีตไกลๆ)
    last_processed_date = context.cursor or '1970-01-01T00:00:00+07:00'
    context.log.info(f"🔍 [Check] Sensor กำลังหาข้อมูลที่ใหม่กว่า: {last_processed_date}")
    
    try:
        with psycopg.connect(CONN_STR) as conn:
            conn.execute("SET TIME ZONE 'Asia/Bangkok';")
            with conn.cursor() as cur:
                # 2. Query หาเฉพาะแถวที่ "ใหม่กว่า" เวลาที่เคยรันล่าสุด
                cur.execute("""
                        SELECT 
                        COUNT(*), 
                        to_char(MAX(upload_date), 'YYYY-MM-DD HH24:MI:SS.MS TZHTZM')
                    FROM stg_userprofile 
                    WHERE upload_date > %s::timestamptz;
                """, (last_processed_date,))
                
                result = cur.fetchone()
                
                if result and result[0] > 0:
                    row_count = result[0]

                    cursor_to_save = result[1]
                    safe_run_key = cursor_to_save.replace(' ', '_').replace(':', '').replace('+', '')
                    
                    context.log.info(f"🚨 เจอข้อมูลใหม่ {row_count} รายการที่เพิ่งเข้ามาหลังเวลา {last_processed_date}")
                    
                    # 3. สั่ง Trigger Job พร้อมแนบเวลาไปให้ Asset รู้ว่าต้องจัดการก้อนไหน
                    yield RunRequest(
                        run_key=f"stg_user_{safe_run_key}",
                        run_config={
                            "ops": {
                                "user_profile_silver": { # ชื่อ Op/Asset ของเรา
                                    "config": {
                                        # ส่งช่วงเวลาไปให้ Asset query
                                        "start_after": last_processed_date, 
                                        "end_at": cursor_to_save
                                    }
                                }
                            }
                        },
                        cursor = cursor_to_save
                    )
                    
                    # 4. อัปเดต Cursor เลื่อนจุด Watermark ไปที่เวลาล่าสุด
                    # รอบหน้า Sensor จะได้ไม่เอาข้อมูลเก่ามารันซ้ำ
                    context.update_cursor(cursor_to_save)

    except Exception as e:
        context.log.error(f"Sensor Error: {e}")