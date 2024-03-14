# FROM apache/airflow:2.1.1
FROM apache/airflow:2.7.2
# FROM apache/airflow:2.8.0
USER root

# Install OpenJDK-11
RUN apt update && \
	apt-get install -y openjdk-11-jdk && \
	apt-get install -y ant && \
	apt-get install -y iputils-ping && \
	apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow
COPY requirements.txt .
# RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
RUN pip install --no-cache-dir -r requirements.txt