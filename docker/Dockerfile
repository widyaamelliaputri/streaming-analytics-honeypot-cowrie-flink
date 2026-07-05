FROM apache/flink:1.17.2

USER root

# Install Python dan dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install PyFlink dan library yang dibutuhkan
RUN pip3 install --no-cache-dir \
    apache-flink==1.17.2 \
    numpy \
    pandas \
    scikit-learn \
    matplotlib \
    seaborn \
    joblib \
    pymongo \
    psycopg2-binary \
    requests 

# Create directory untuk custom jobs
RUN mkdir -p /opt/flink/usrlib

# Set working directory
WORKDIR /opt/flink

# Buat symlink python -> python3 agar permanen
RUN ln -s /usr/bin/python3 /usr/bin/python

# Buat direktori data
RUN mkdir -p /opt/data/models /opt/data/processed

# Kembali ke user flink
USER flink

ENTRYPOINT ["/docker-entrypoint.sh"]