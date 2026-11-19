FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyspark==3.5.8 psycopg2-binary==2.9.10

WORKDIR /app
COPY src/ src/
ENV PYTHONPATH=/app

# Resolve the Kafka connector during the build so a restart uses cached jars.
RUN printf 'from pyspark.sql import SparkSession\nSparkSession.builder.getOrCreate().stop()\n' > /tmp/resolve-kafka.py \
    && spark-submit --master 'local[1]' \
        --conf spark.jars.ivy=/opt/spark-ivy \
        --conf spark.driver.host=127.0.0.1 \
        --conf spark.driver.bindAddress=127.0.0.1 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 /tmp/resolve-kafka.py \
    && rm /tmp/resolve-kafka.py

CMD ["spark-submit", "--master", "local[2]", "--conf", "spark.jars.ivy=/opt/spark-ivy", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8", "src/streaming.py"]
