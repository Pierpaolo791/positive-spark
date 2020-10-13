FROM maven:latest

ENV SPARK_VERSION=2.4.7
ENV SPARK_DIR=/opt/spark
ENV PATH $SPARK_DIR/bin:$PATH
ENV POSITIVE_SPARK_STREAMING "Positive Spark Streaming"
ENV POSITIVE_SPARK_DIR "/opt/positive-spark/"

ADD spark-${SPARK_VERSION}-bin-hadoop2.7.tgz /opt
RUN ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop2.7 ${SPARK_DIR} 

COPY ${POSITIVE_SPARK_STREAMING} ${POSITIVE_SPARK_DIR}
WORKDIR ${POSITIVE_SPARK_DIR}
RUN mvn package
RUN cp target/positive* ${POSITIVE_SPARK_DIR}


COPY spark-starter.sh ${POSITIVE_SPARK_DIR}
WORKDIR ${POSITIVE_SPARK_DIR}
CMD ./spark-starter.sh