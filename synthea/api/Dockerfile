FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install dependencies
RUN pip install flask requests docker-compose

# Copy API code
COPY api.py .

# Create directory for job files
RUN mkdir -p /tmp/synthea_jobs

# Expose port
EXPOSE 5082

# Run API
CMD ["python", "api.py"]
