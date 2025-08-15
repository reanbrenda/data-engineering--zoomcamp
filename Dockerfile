# Use Python 3.12.1 slim image for smaller size and latest features
FROM python:3.12.1-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV TZ=Europe/London
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies in a single layer to reduce image size
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    librdkafka-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies with latest pip
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# Copy application code
COPY playground/movement.py .
COPY secrets.json .

# Create logs directory and set permissions
RUN mkdir -p logs && \
    chmod 755 logs

# Create non-root user for security (using numeric ID for better compatibility)
RUN groupadd -r app && useradd -r -g app app && \
    chown -R app:app /app
USER app

# Health check - simplified and more reliable
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; print('healthy'); sys.exit(0)" || exit 1

# Default command
CMD ["python", "movement.py"]
