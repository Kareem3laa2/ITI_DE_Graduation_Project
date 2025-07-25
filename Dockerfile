FROM bitnami/spark:3.1.1

USER root

# Install system & Python dependencies
RUN sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list && \
    sed -i 's|http://security.debian.org|http://archive.debian.org/debian-security|g' /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y python3 python3-pip openjdk-11-jdk curl && \
    pip3 install --upgrade pip && \
    pip3 install numpy pandas sqlalchemy psycopg2-binary scikit-learn pyspark findspark imbalanced-learn


# Add the PostgreSQL JDBC driver
COPY jars/postgresql-42.7.7.jar /opt/bitnami/spark/jars/
COPY jars/snowflake-jdbc-3.13.31.jar /opt/bitnami/spark/jars/
COPY jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar /opt/bitnami/spark/jars/
COPY jars/spark-excel_2.12-3.3.1_0.18.6-beta1.jar /opt/bitnami/spark/jars/

USER 1001
