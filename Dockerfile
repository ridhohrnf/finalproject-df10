FROM apache/airflow:2.6.3

# Install OpenJDK 11
USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Switch back to the airflow user
USER airflow

RUN pip install apache-airflow-providers-apache-spark
RUN pip install joblibspark