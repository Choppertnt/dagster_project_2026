FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /opt/dagster/app

RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN uv pip install --system --no-cache -r pyproject.toml
RUN uv pip install --system dagster-postgres dagster-k8s

# 🔄 [จุดที่แก้] ก๊อปปี้โครงสร้างโฟลเดอร์ใหม่ทั้งหมดเข้า Container
COPY ecommerce/ ./ecommerce/
COPY google_health/ ./google_health/
COPY repo_health.py .
COPY repo.py .

# กำหนด DAGSTER_HOME
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# สร้าง Folder และปรับสิทธิ์ให้ Group 0 เขียนได้ (OpenShift Support)
RUN mkdir -p $DAGSTER_HOME && \
    chgrp -R 0 /opt/dagster && \
    chmod -R g+rwX /opt/dagster

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]