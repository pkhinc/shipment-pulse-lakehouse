FROM apache/spark:4.1.3-python3
USER root
RUN python3 -m pip install "pyspark[pipelines]==4.1.3"