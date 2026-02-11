from dagster import run_failure_sensor, RunFailureSensorContext, DefaultSensorStatus
import requests

# --- CONFIG LINE OA ---
LINE_CHANNEL_ACCESS_TOKEN = "uOmAHmG3HuvTEEtyZe2VyNnhOTdjWfqMbtNSZuhUG+4u8ubqzK4ygeEPVxfWVSg48mUe3zWYwekMAV1HKdgwaR1LlMa7kf/vhDwg/vJCBKEbrxKtMKFY7YtnmlKq8tiZHQiSCDnPTXNq2tvUYJ+shAdB04t89/1O/w1cDnyilFU="
LINE_ADMIN_USER_ID = "Ue6bdf5351b582d3afeb23f236603b531"

def send_line_oa_push(message):
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
        requests.post(url, headers=headers, json=payload)
    except Exception as e:
        print(f"❌ Failed to send LINE alert: {e}")

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
    error_msg = str(context.failure_event.message)
    if len(error_msg) > 500:
        error_msg = error_msg[:500] + "... (ดูต่อใน Dagster)"

    # 3. สร้างข้อความแจ้งเตือน
    alert_msg = (
        f"☠️ JOB FAILED! ☠️\n"
        f"--------------------\n"
        f"🔴 Job: {job_name}\n"
        f"🆔 Run ID: {run_id}\n"
        f"--------------------\n"
        f"📄 Error: {error_msg}"
    )

    # 4. ส่ง LINE
    send_line_oa_push(alert_msg)