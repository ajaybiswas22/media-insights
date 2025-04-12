FROM bitnami/spark:3.5.5

USER root

# Install pip
RUN apt-get update && apt-get install -y python3-pip && apt-get clean

# Copy requirements and install
COPY requirements.txt /tmp/requirements.txt
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY log4j.properties /opt/bitnami/spark/conf/log4j.properties

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Set environment variable
ENV SPARK_JARS_PACKAGES=io.delta:delta-core_2.12:2.4.0
ENV SPARK_MASTER=spark://spark-master:7077

USER 1001
