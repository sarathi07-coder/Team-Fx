FROM python:3.11-slim

WORKDIR /app

# Create non-root user (security requirement)
RUN adduser --disabled-password --gecos "" appuser

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY loader.py .

# Run as non-root
USER appuser

CMD ["python", "loader.py"]
