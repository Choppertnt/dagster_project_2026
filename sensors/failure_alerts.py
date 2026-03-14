from dagster import run_failure_sensor, RunFailureSensorContext, DefaultSensorStatus
import requests
import os

# --- CONFIG LINE OA ---


def send_line_oa_push(context, message):
    LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS")

    LINE_ADMIN_USER_ID = os.getenv("LINE_ADMIN_USER")


    # 📍 2. ดักเช็คก่อนเลยว่าลืมใส่ Environment Variables ไหม (ถ้าลืมให้ฟ้อง Error แดงๆ ทันที)
    if not LINE_CHANNEL_ACCESS_TOKEN or not LINE_ADMIN_USER_ID:
        context.log.error("❌ หา Env: LINE_CHANNEL_ACCESS หรือ LINE_ADMIN_USER ไม่เจอ! โปรดเช็ค Config บน K3s")
        return

    url = 'https://api.line.me/v2/bot/message/push'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {LINE_CHANNEL_ACCESS_TOKEN}'
    }
    payload = {
        "to": LINE_ADMIN_USER_ID,
        "messages": [{"type": "text", "text": message}]
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        
        # 📍 3. บังคับเช็ค Status Code (เพราะปกติ requests จะไม่พ่น Error ถ้ารหัส Token ผิด)
        if response.status_code != 200:
            context.log.error(f"❌ ส่ง LINE ไม่สำเร็จ! HTTP Status: {response.status_code}")
            context.log.error(f"💬 สาเหตุจาก LINE: {response.text}")
        else:
            context.log.info("✅ ส่งแจ้งเตือนเข้า LINE OA สำเร็จ!")
            
    except Exception as e:
        context.log.error(f"❌ ระบบเครือข่ายมีปัญหาตอนยิงไปหา LINE: {e}")

# --- SENSOR LOGIC ---
@run_failure_sensor(name="line_oa_failure_sensor", default_status=DefaultSensorStatus.RUNNING)
def line_oa_failure_sensor(context: RunFailureSensorContext):
    """
    Sensor ตัวนี้จะทำงานทันที เมื่อมี Job ไหนก็ตามใน Repo นี้ 'Failed'
    """
    
    # 1. ดึงข้อมูล Job ที่พัง
    job_name = context.dagster_run.job_name
    run_id = context.dagster_run.run_id
    
    # 2. ดึงข้อความ Error (ตัดให้สั้นหน่อย เดี๋ยว LINE เต็ม)
    # 📍 ป้องกัน error_msg เป็น None ด้วยการดักเงื่อนไข
    error_msg = str(context.failure_event.message) if context.failure_event else "ไม่มีข้อความ Error ระบุไว้"
    if len(error_msg) > 500:
        error_msg = error_msg[:500] + "... (ดูต่อใน Dagster)"

    # 3. สร้างข้อความแจ้งเตือน
    alert_msg = (
        f"☠️ JOB FAILED! ☠️\n"
        f"--------------------\n"
        f"🔴 Job: {job_name}\n"
        f"🆔 Run ID: {run_id[:8]}\n" # ตัดให้สั้นลงเหลือ 8 ตัว จะได้ดูสวยๆ
        f"--------------------\n"
        f"📄 Error: {error_msg}"
    )

    # 4. ส่ง LINE (📍 โยน context ตามเข้าไปด้วย)
    send_line_oa_push(context, alert_msg)