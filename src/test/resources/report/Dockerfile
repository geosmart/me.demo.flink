FROM maven:3.6-jdk-8-slim AS builder

FROM apache/flink:1.12.2-scala_2.12-java8

# Download connector libraries
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.12/1.12.2/flink-sql-connector-kafka_2.12-1.12.2.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc_2.12/1.12.2/flink-connector-jdbc_2.12-1.12.2.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-csv/1.12.2/flink-csv-1.12.2.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar;

COPY  ../../../../build/libs/*.jar /opt/flink/usrlib/spend-report.jar

RUN echo "execution.checkpointing.interval: 10s" >> /opt/flink/conf/flink-conf.yaml; \
    echo "pipeline.object-reuse: true" >> /opt/flink/conf/flink-conf.yaml; \
    echo "pipeline.time-characteristic: EventTime" >> /opt/flink/conf/flink-conf.yaml; \
    echo "taskmanager.memory.jvm-metaspace.size: 256m" >> /opt/flink/conf/flink-conf.yaml;