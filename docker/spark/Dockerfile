# Spark 4.2 + Delta Lake + S3A (to read/write the RustFS lake), jars baked in at build time
# so containers never download dependencies at startup.
FROM apache/spark:4.2.1-python3

ARG MAVEN=https://repo1.maven.org/maven2
ARG HADOOP_VERSION=3.5.0
# Must match the AWS SDK v2 version Hadoop 3.5.0 was built against (hadoop-project pom: aws-java-sdk-v2.version)
ARG AWS_SDK_VERSION=2.35.4
ARG DELTA_VERSION=4.4.1
# Runtime dependency of Hadoop 3.5's S3A client (hadoop-project pom: amazon-s3-analyticsaccelerator-s3.version)
ARG S3_ACCEL_VERSION=1.3.1
# JDBC driver so executors can load curated tables into the Postgres warehouse
ARG PG_JDBC_VERSION=42.7.15

USER root
RUN cd /opt/spark/jars \
 && curl -fsSLO ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/${S3_ACCEL_VERSION}/analyticsaccelerator-s3-${S3_ACCEL_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/io/delta/delta-spark_2.13/${DELTA_VERSION}/delta-spark_2.13-${DELTA_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar \
 && chmod 644 /opt/spark/jars/*.jar
USER spark
