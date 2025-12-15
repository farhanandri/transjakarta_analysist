FROM apache/airflow:2.3.4-python3.9

USER root
RUN apt-get update

USER root
RUN apt-get install -y openjdk-11-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER root
RUN apt-get install -y libgdal-dev libgeos-dev libproj-dev

RUN apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    great-expectations==0.18.19 \
    kagglehub \
    pyspark==3.1.2 \
    pymongo \
    requests==2.28.1\
    geopandas\
    pandas\
    numpy\
    python-dateutil\
    