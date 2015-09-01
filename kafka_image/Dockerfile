FROM ubuntu:14.04
MAINTAINER Daniel Gergely
LABEL Description="This image contains zookeeper and kafka servers" Vendor="Daniel Gergely" Version="1.0"

RUN apt-get update && apt-get install -y openjdk-7-jre-headless curl
RUN curl -O http://apache.arvixe.com/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz
RUN tar xf kafka_2.10-0.8.2.1.tgz -C /opt

ENV KAFKA_HOME /opt/kafka_2.10-0.8.2.1

EXPOSE 2181 2888 3888 9092

ADD server.properties /opt/kafka_config/server.properties 
ADD zookeeper.properties /opt/kafka_config/zookeeper.properties
ADD start-server.sh /opt/kafka_run/start-server.sh

RUN chmod +x /opt/kafka_run/start-server.sh

CMD /opt/kafka_run/start-server.sh


