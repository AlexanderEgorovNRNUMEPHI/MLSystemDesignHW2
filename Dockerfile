FROM apache/airflow:2.10.3

USER root
RUN apt-get update && \
    apt install -y default-jdk && \
    apt install wget
COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8
ENV JAVA_HOME /usr/local/openjdk-8
RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-8/bin/java 1

RUN wget -P /opt/applications --no-check-certificate https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN wget -P /opt/applications --no-check-certificate https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.540/aws-java-sdk-bundle-1.12.540.jar

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --no-cache-dir -r  /requirements.txt


COPY .env /opt/airflow/.env
COPY ./dags/app.py /opt/applications/app.py
