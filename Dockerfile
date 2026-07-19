# Use your exact version to ensure compatibility
FROM apache/airflow:3.0.3

USER airflow

# Copy your requirements file
COPY requirements.txt /requirements.txt

# Install dependencies using the specific Airflow constraints to prevent version conflicts
RUN pip install --no-cache-dir -r /requirements.txt
