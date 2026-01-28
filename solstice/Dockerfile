# Solstice Base Runtime Image
# Includes: Python 3.12, JVM 17, FFmpeg, Ray, tansu-py
# Based on Ubuntu 24.04
# 
# This is a base image - solstice code is NOT included.
# Mount or copy solstice code at runtime.
#
# Build from nurion root directory:
#   docker build -f solstice/Dockerfile -t solstice-base .

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Use mirrors to avoid network issues with archive.ubuntu.com
RUN sed -i 's|http://archive.ubuntu.com|http://mirrors.edge.kernel.org|g' /etc/apt/sources.list.d/ubuntu.sources && \
    sed -i 's|http://security.ubuntu.com|http://mirrors.edge.kernel.org|g' /etc/apt/sources.list.d/ubuntu.sources

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Basic tools
    curl wget software-properties-common gnupg tini gosu libnss-wrapper git ca-certificates \
    # JVM (OpenJDK 17)
    openjdk-17-jdk maven \
    # Python 3.12
    python3.12 python3.12-dev python3.12-venv python3-pip \
    # FFmpeg for video processing
    ffmpeg \
    # Build dependencies for Rust packages
    build-essential pkg-config libssl-dev \
    # Network debugging tools
    net-tools iproute2 iputils-ping dnsutils tcpdump netcat-openbsd \
    # System monitoring tools
    htop sysstat strace lsof procps \
    && rm -rf /var/lib/apt/lists/*

# Setup Python alternatives and remove externally-managed marker
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.12 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1 && \
    rm -f /usr/lib/python3.12/EXTERNALLY-MANAGED

# Install uv (fast Python package manager)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Setup environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="/root/.local/bin:${JAVA_HOME}/bin:${PATH}"

WORKDIR /app
# Install Python runtime dependencies (minimal set for Ray + data processing)
RUN uv pip install --system --no-cache \
    "ray[all]>=2.52.0" \
    pyspark==3.5.6 \
    pyarrow>=18.1.0 \
    pandas>=2.0.0 \
    click>=8.1.7 \
    "fsspec[s3]>=2024.6.0" \
    pylance>=0.38.0 \
    s3fs>=2024.6.0 \
    confluent-kafka>=2.3.0 \
    # Python profiling and monitoring tools
    py-spy \
    memray \
    psutil

# Copy only what's needed for building (tansu-py and raydp from lib/)
COPY lib/tansu-py /app/build/tansu-py
COPY lib/raydp /app/build/raydp

# Install Rust, build tansu-py, then cleanup Rust completely (all in one layer)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    export PATH="/root/.cargo/bin:$PATH" && \
    cargo install maturin && \
    cd /app/build/tansu-py && \
    maturin build --release && \
    uv pip install --system --no-cache target/wheels/*.whl && \
    # Cleanup Rust and source completely
    rm -rf /app/build/tansu-py && \
    rm -rf /root/.cargo && \
    rm -rf /root/.rustup

# Download extra JARs for Spark S3/Lance support
ENV M=https://repo1.maven.org/maven2
RUN PYSPARK_JARS=$(python -c "import pyspark; print(pyspark.__path__[0])")/jars && \
    rm -f ${PYSPARK_JARS}/arrow-*.jar && \
    wget -q ${M}/org/apache/spark/spark-hadoop-cloud_2.12/3.5.6/spark-hadoop-cloud_2.12-3.5.6.jar \
         ${M}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
         ${M}/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
         ${M}/org/lance/lance-spark-bundle-3.5_2.12/0.1.3-beta.7/lance-spark-bundle-3.5_2.12-0.1.3-beta.7.jar \
         -P ${PYSPARK_JARS}/

# Build and install RayDP (uv build will automatically run Maven via _build_hooks.py)
RUN cd /app/build/raydp && \
    uv build && \
    uv pip install --system --no-cache dist/*.whl && \
    # Cleanup build artifacts
    rm -rf /app/build && \
    rm -rf /root/.m2


# Verify installations and remove build dependencies
RUN python -c "import ray; print(f'Ray: {ray.__version__}')" && \
    python -c "import tansu_py; print('tansu_py: OK')" && \
    python -c "import pyarrow; print(f'PyArrow: {pyarrow.__version__}')" && \
    python -c "import raydp; print(f'RayDP: {raydp.__version__}')" && \
    ffmpeg -version | head -1 && \
    java -version 2>&1 | head -1 

# Default working directory
WORKDIR /app

# No entrypoint - this is a base image
CMD ["python"]
