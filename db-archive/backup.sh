#!/bin/bash

# === CONFIG ===
BACKUP_DIR="/db-archive"
ARCHIVE_DIR="$BACKUP_DIR/archive"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M")
LATEST_BACKUP="$BACKUP_DIR/backup-latest.sql"
ARCHIVED_BACKUP="$ARCHIVE_DIR/backup-$TIMESTAMP.sql"
DOCKER_CONTAINER="yt-scraper-db-1"
DB_NAME="mydatabase"
DB_USER="myuser"

# === ENSURE DIRECTORIES EXIST ===
mkdir -p "$ARCHIVE_DIR"

# === ARCHIVE EXISTING BACKUP IF PRESENT ===
if [ -f "$LATEST_BACKUP" ]; then
    mv "$LATEST_BACKUP" "$ARCHIVED_BACKUP"
fi

# === CREATE NEW BACKUP ===
docker exec "$DOCKER_CONTAINER" pg_dump -U "myuser" "mydatabase" > "$LATEST_BACKUP"
