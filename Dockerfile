FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /opt/dagster/app

RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN uv pip install --system --no-cache -r pyproject.toml
RUN uv pip install --system dagster-postgres dagster-k8s

# Copy เฉพาะโค้ด Assets และ Repo
COPY assets/ ./assets/
COPY jobs/ ./jobs/
COPY repo.py .

# กำหนด DAGSTER_HOME
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# ✅ จุดที่ต้องแก้: สร้าง Folder และปรับสิทธิ์ให้ Group 0 เขียนได้ (OpenShift Support)
RUN mkdir -p $DAGSTER_HOME && \
    # เปลี่ยนเจ้าของ Group เป็น 0 (Root Group) ทั้งก้อน /opt/dagster
    chgrp -R 0 /opt/dagster && \
    # ให้สิทธิ์ Group 0 อ่าน/เขียน/Execute ได้
    chmod -R g+rwX /opt/dagster

EXPOSE 3000