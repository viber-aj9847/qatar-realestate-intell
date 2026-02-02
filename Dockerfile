# Option B: Docker deployment - Playwright Python image with Chromium pre-installed
# Use this if Option A (native build) fails with "Executable doesn't exist"
# In Render: change service to Docker type; set DATABASE_URL, SECRET_KEY in Environment

FROM mcr.microsoft.com/playwright/python:v1.40.0-noble

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Render sets PORT; default 10000 for web services
ENV PORT=10000
EXPOSE 10000

CMD gunicorn app:app --bind 0.0.0.0:${PORT}
