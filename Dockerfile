FROM apache/airflow:2.10.5

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Definimos JAVA_HOME globalmente para que Spark lo encuentre siempre
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# En algunas distros es /usr/lib/jvm/java-1.17.0-openjdk-amd64,
# pero la que pusiste suele ser el estándar en Debian.

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.10.0 \
    pyspark==4.1.1
