FROM docker.io/bitnami/spark:3.3

ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
ENV PATH="${HOME}/.local/bin/:$PATH"

USER root
RUN apt-get update 
RUN apt-get install wget -qqq

# install postgres jdbc
RUN wget https://jdbc.postgresql.org/download/postgresql-42.5.1.jar -P /opt/bitnami/spark/jars

USER 1001

COPY ./spark/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt