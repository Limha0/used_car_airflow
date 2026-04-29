FROM apache/airflow:2.8.0-python3.10  

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
  wget gnupg fonts-liberation libnss3 libatk-bridge2.0-0 \
  libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 libgbm1 \
  libasound2 libpangocairo-1.0-0 libpango-1.0-0 libatk1.0-0 \
  libcups2 libdbus-1-3 libdrm2 libxss1 libgtk-3-0 \
&& rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt && \
  playwright install chromium