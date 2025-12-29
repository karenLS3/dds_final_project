# Use official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=5001

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Install Python dependencies first (cache layer)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY . /app

# Expose port
EXPOSE 5001

# Healthcheck (optional)
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s CMD curl -f http://localhost:${PORT}/health || exit 1

# Default command: use gunicorn for production
CMD ["gunicorn", "-w", "3", "-b", "0.0.0.0:5001", "flask_app:app"]
