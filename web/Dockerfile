# Version: 1.0
FROM python:3.12-slim-bookworm
# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
# Create the app user and group
COPY wait-for-it.sh .
RUN chmod +x wait-for-it.sh
RUN groupadd -g 1001 app
RUN useradd -m -u 1001 -g app app
# Project files and settings
ARG PROJECT=iluminaconciencia-website
ARG PROJECT_DIR=/var/www/${PROJECT}
RUN mkdir -p $PROJECT_DIR
WORKDIR $PROJECT_DIR
# Install dependencies
RUN apt-get update
RUN apt-get install -y g++ netcat-openbsd
RUN apt-get install -y bash libpq-dev python3-dev make redis
RUN python3 -m pip install --upgrade pip
# Files web
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install numpy --config-settings=setup-args="-Dallow-noblas=true"
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    build-essential gcc pkg-config default-libmysqlclient-dev \
 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt
# Remove folders
#RUN rm -R assets
#RUN rm -R media
#RUN rm -R data
# Chown all the files to the app user
RUN chown -R app:app $PROJECT_DIR
RUN chmod -R 755 $PROJECT_DIR
ENV DJANGO_SETTINGS_MODULE=web.settings
# Change to the app user
USER app
# Server
EXPOSE 8000
#CMD ["python", "manage.py", "collectstatic", "--noinput"]
#CMD ["gunicorn", "--bind", "0.0.0.0:8002", "--workers", "1", "--timeout", "6000", "web.wsgi:application"]
#CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
