FROM apache/airflow:2.9.1-python3.10

# 1. Install Java as root
USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl && \
    apt-get clean

# 2. Set environment variables
ENV SPARK_VERSION=3.5.1 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="/opt/spark/bin:$PATH"

# 3. Download and install Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    ln -s ${SPARK_HOME}/bin/spark-submit /usr/bin/spark-submit

# 4. Switch to airflow user BEFORE pip install
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark
