import datetime
import os
from zoneinfo import ZoneInfo
from dagster import AssetExecutionContext, asset , Config
import requests
from typing import Optional



BACKEND_API_URL = os.getenv("BACKEND_API_URL", "http://localhost:8000")

class HeartRateSyncConfig(Config):
  start_time_override: Optional[str] = (None  
  )
  end_time_override: Optional[str] = None  
  lookback_minutes: int = 20 

def get_fresh_access_token():
  client_id = os.getenv("CLIENT_ID")
  client_secret = os.getenv("CLIENT_SECRET")
  refresh_token = os.getenv("REFRESH_TOKEN")

  token_url = "https://oauth2.googleapis.com/token"
  payload = {
      "client_id": client_id,
      "client_secret": client_secret,
      "refresh_token": refresh_token,
      "grant_type": "refresh_token",
  }

  response = requests.post(token_url, data=payload)

  # 🌟 เพิ่มบรรทัดนี้เพื่อปรินท์คำฟ้องที่แท้จริงจาก Google ออกมาดูใน Log
  if response.status_code != 200:
    print(f"🚨 Google บอกเหตุผลที่พังไว้ว่า: {response.json()}")

  response.raise_for_status()
  tokens = response.json()
  return tokens["access_token"]




# ==========================================
# 🚀 3. Dagster Asset: Sync Heart Rate Pipeline
# ==========================================
@asset(group_name="google_health_pipeline")
def sync_heart_rate_asset(
    context: AssetExecutionContext, config: HeartRateSyncConfig
):
  context.log.info("🚀 Dagster Asset: เริ่มดึงข้อมูล Heart Rate...")

  # 1. ขอ Access Token ใหม่สดๆ
  try:
    access_token = get_fresh_access_token()
    context.log.info("🔄 ต่ออายุ Access Token สำเร็จแล้ว!")
  except Exception as e:
    context.log.error(f"❌ ไม่สามารถต่ออายุ Access Token ได้: {e}")
    raise e

  # 2. ตั้งค่า Timezone เวลาไทย
  thai_tz = ZoneInfo("Asia/Bangkok")
  now_thai = datetime.datetime.now(thai_tz)

  if config.start_time_override and config.end_time_override:
    start_dt = datetime.datetime.fromisoformat(config.start_time_override)
    end_dt = datetime.datetime.fromisoformat(config.end_time_override)
    context.log.info(
        f"⚙️ ใช้ช่วงเวลาแบบ Custom Config: {start_dt.isoformat()} ถึง"
        f" {end_dt.isoformat()}"
    )
  else:
    end_dt = now_thai
    start_dt = now_thai - datetime.timedelta(minutes=config.lookback_minutes)
    context.log.info(
        f"🕒 ใช้ช่วงเวลาแบบ Default ({config.lookback_minutes} นาทีล่าสุด):"
        f" {start_dt.strftime('%Y-%m-%d %H:%M:%S')} ถึง"
        f" {end_dt.strftime('%Y-%m-%d %H:%M:%S')} (เวลาไทย)"
    )

  # ==========================================
  # [E] EXTRACT & [L] LOAD (Chunking + Sub-batching)
  # ==========================================
  chunk_size = datetime.timedelta(hours=3)  # แบ่งก้อนดึงทีละ 3 ชั่วโมง
  curr_start = start_dt
  google_url = (
      "https://health.googleapis.com/v4/users/me/dataTypes/heart-rate/dataPoints"
  )
  headers = {"Authorization": f"Bearer {access_token}"}
  backend_endpoint = f"{BACKEND_API_URL}/api/v1/health/sync-heart-rate"

  total_inserted = 0

  while curr_start < end_dt:
    curr_end = min(curr_start + chunk_size, end_dt)
    filter_query = (
        f'heart_rate.sample_time.physical_time >= "{curr_start.isoformat()}"'
        f' AND heart_rate.sample_time.physical_time < "{curr_end.isoformat()}"'
    )

    context.log.info(
        f"🔎 ดึง Chunk ช่วงเวลา: {curr_start.strftime('%Y-%m-%d %H:%M')} ถึง"
        f" {curr_end.strftime('%Y-%m-%d %H:%M')}..."
    )

    chunk_data_points = []
    page_token = None

    # --- Step A: ดึงข้อมูลจาก Google Health API (Pagination) ---
    while True:
      params = {"filter": filter_query}
      if page_token:
        params["pageToken"] = page_token

      try:
        response = requests.get(
            google_url, headers=headers, params=params, timeout=20
        )
      except requests.exceptions.RequestException as err:
        context.log.error(f"❌ Network Error จาก Google: {err}")
        break

      if response.status_code != 200:
        context.log.error(f"❌ Error จาก Google API: {response.text}")
        break

      data = response.json()
      if "dataPoints" in data:
        chunk_data_points.extend(data["dataPoints"])

      page_token = data.get("nextPageToken")
      if not page_token:
        break

    # --- Step B: ทยอยยิงส่ง Backend ทีละ 1,000 รายการ (Sub-batching) ---
    if chunk_data_points:
      BATCH_SIZE = 1000
      total_chunk_data = len(chunk_data_points)
      context.log.info(
          f"📦 Chunk นี้พบ {total_chunk_data} รายการ กำลังทยอยส่งให้"
          f" Backend ทีละ {BATCH_SIZE} รายการ..."
      )

      for i in range(0, total_chunk_data, BATCH_SIZE):
        sub_batch = chunk_data_points[i : i + BATCH_SIZE]
        payload = {"dataPoints": sub_batch}

        try:
          backend_response = requests.post(
              backend_endpoint, json=payload, timeout=60
          )
          backend_response.raise_for_status()
          res_json = backend_response.json()
          inserted = res_json.get("inserted", 0)
          total_inserted += inserted
          context.log.info(
              f"  └─ 🟢 Batch ที่ {i // BATCH_SIZE + 1} บันทึกสำเร็จ: เพิ่มใหม่"
              f" {inserted} รายการ"
          )
        except requests.exceptions.RequestException as e:
          context.log.error(f"❌ ส่ง Sub-batch ไป Backend ล้มเหลว: {e}")
          raise Exception(f"Backend Sync Failed on Sub-batch: {e}")

    curr_start = curr_end  # ขยับไป Chunk 3 ชั่วโมงถัดไป

  context.log.info(
      f"🎉 เสร็จสิ้นการ Sync ทุกช่วงเวลา! บันทึกข้อมูลใหม่รวมทั้งสิ้น:"
      f" {total_inserted} รายการ"
  )
  return {"status": "success", "total_inserted": total_inserted}

# ops:
#   sync_heart_rate_asset:
#     config:
#       start_time_override: "2026-07-28T00:00:00+07:00"
#       end_time_override: "2026-07-28T23:59:59+07:00"


@asset(group_name="google_health_pipeline")
def sync_sleep_asset(context: AssetExecutionContext):
  context.log.info(
      "🚀 Dagster Asset: เริ่มดึงข้อมูล Sleep (การนอนหลับ) + ระบบ Pagination..."
  )

  # 1. ขอ Access Token ใหม่จาก Refresh Token
  try:
    access_token = get_fresh_access_token()
  except Exception as e:
    context.log.error(f"❌ ไม่สามารถต่ออายุ Access Token ได้: {e}")
    raise e

  google_url = (
      "https://health.googleapis.com/v4/users/me/dataTypes/sleep/dataPoints"
  )
  headers = {"Authorization": f"Bearer {access_token}"}

  # ==========================================
  # [E] EXTRACT: ดึงข้อมูล Sleep โดยไม่ใส่ filter (กวาดทุก Session)
  # ==========================================
  all_data_points = []
  page_token = None

  while True:
    params = {}
    if page_token:
      params["pageToken"] = page_token

    try:
      # 🌟 ใส่ timeout=20 ป้องกันการค้าง
      response = requests.get(
          google_url, headers=headers, params=params, timeout=20
      )
    except requests.exceptions.RequestException as err:
      context.log.error(f"❌ Network Error จาก Google Sleep API: {err}")
      return {"status": "error", "message": f"Network Error: {err}"}

    if response.status_code != 200:
      context.log.error(f"❌ Error จาก Google Sleep API: {response.text}")
      return {"status": "error", "message": "Failed to fetch sleep from Google"}

    data = response.json()

    if "dataPoints" in data:
      all_data_points.extend(data["dataPoints"])

    page_token = data.get("nextPageToken")
    if not page_token:
      break

  if not all_data_points:
    context.log.info("✨ ไม่มีข้อมูล Sleep ในระบบ")
    return {"status": "success", "new_records": 0}

  context.log.info(
      f"📦 ดึงข้อมูล Sleep ดิบสำเร็จทั้งหมด: พบ {len(all_data_points)} รายการ"
      " กำลังส่งต่อให้ Backend..."
  )

  payload = {"dataPoints": all_data_points}

  # ==========================================
  # [L] LOAD: ยิง HTTP POST ไปให้ FastAPI Backend
  # ==========================================
  backend_endpoint = f"{BACKEND_API_URL}/api/v1/health/sync-sleep"

  try:
    # 🌟 ใส่ timeout=30 ป้องกัน Dagster ถือสายค้าง
    backend_response = requests.post(backend_endpoint, json=payload, timeout=30)
    backend_response.raise_for_status()

    result = backend_response.json()
    context.log.info(
        f"✅ ส่งข้อมูล Sleep ให้ Backend สำเร็จ! Backend ตอบกลับมาว่า: {result}"
    )
    return result

  except requests.exceptions.RequestException as e:
    context.log.error(f"❌ เกิดข้อผิดพลาดในการส่งข้อมูล Sleep ไป Backend: {e}")
    raise Exception(f"Backend Sleep Sync Failed: {e}")