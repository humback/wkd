# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and source code
COPY requirements.txt ./
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
COPY . .



# Expose port
EXPOSE 5105

# Run the Flask app
CMD ["python", "index.py"]
