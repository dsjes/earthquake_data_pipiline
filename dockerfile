# download jdk image
FROM openjdk:11-jdk as jdk-stage

# download airflow image
FROM apache/airflow:2.8.4
USER root

RUN apt-get update && \
    apt-get install -y procps

# copy JDK image to  Airflow image
COPY --from=jdk-stage /usr/local/openjdk-11 /usr/local/openjdk-11

USER airflow
COPY . /opt/airflow
WORKDIR /opt/airflow
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

ENV JAVA_HOME=/usr/local/openjdk-11
ENV PATH=$PATH:$JAVA_HOME/bin