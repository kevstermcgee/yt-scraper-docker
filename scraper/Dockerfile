FROM mcr.microsoft.com/playwright/python:v1.54.0-noble

WORKDIR /app

# Install required system packages
RUN apt-get update && apt-get install -y \
    libpq-dev gcc \
    # wireguard-tools \
    # iproute2 \
    # curl \
    # bash \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# COPY brazil.conf /etc/wireguard/wg0.conf

# Upgrade pip and related tools
RUN pip install --upgrade pip setuptools wheel

# Copy requirements first for better caching (if uncommented)
COPY requirements.txt .

# Copy over backup script
# COPY db-archive/backup.sh /db-archive/backup.sh

# Install Python dependencies
RUN pip install --no-cache-dir psycopg[binary]

# If you have other dependencies:
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app
COPY . .

# Start the app
# CMD wg-quick up wg0 && python3 main.py
CMD ["python", "main.py"]
