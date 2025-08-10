FROM python:3.11-slim

#WORKDIR /app

# Install OS dependencies required by psycopg2
#RUN apt-get update && \
#    apt-get install -y --no-install-recommends gcc libpq-dev && \
#    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
#COPY requirements.txt .
#RUN pip install --no-cache-dir --upgrade pip \
# && pip install --no-cache-dir -r requirements.txt

# Copy source code
#COPY . .

# Make entrypoint executable
#RUN chmod +x /app/entrypoint.sh

#ENTRYPOINT ["/app/entrypoint.sh"]
#CMD ["all"]
