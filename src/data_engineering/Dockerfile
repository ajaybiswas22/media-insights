FROM bitnami/spark:3.5.5

USER root

# Install pip
RUN apt-get update && apt-get install -y python3-pip && apt-get clean

# Copy requirements and install
COPY requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

USER 1001
