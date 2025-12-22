FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY job_manager.py .
COPY compute.py .
COPY worker.py .

# Create data directories
RUN mkdir -p data/raw data/processed/chunks

# Expose API port
EXPOSE 8000

# Run the API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"] 