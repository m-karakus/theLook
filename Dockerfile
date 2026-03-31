ARG PYTHON_VERSION=3.13
ARG IMAGE_VARIANT=slim-bookworm

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT}

ARG BUILD_DATE="1970-01-01T00:00:00Z"
ARG VERSION="latest"
ARG COMMIT_REF="unknown"

LABEL org.opencontainers.image.created=${BUILD_DATE} \
    org.opencontainers.image.version=${VERSION} \
    org.opencontainers.image.revision=${COMMIT_REF}

ENV APP_HOME=/app
ENV DAGSTER_HOME=/app/dagster_home

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_ROOT_USER_ACTION=ignore
ENV TZ=UTC

ENV DBT_PROJECT_DIR=${APP_HOME}/dbt_project
ENV DBT_PROFILES_DIR=${DBT_PROJECT_DIR}/.dbt

WORKDIR ${APP_HOME}

# Install system deps (Java for PySpark, procps for jps)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create arch-independent symlink for JAVA_HOME
RUN ln -sfn /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Python deps (cached layer)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Pre-download JARs (no internet needed at runtime)
RUN mkpipe install-jars

# Copy project files (.dockerignore filters out unwanted files)
COPY . .

# Make init scripts executable
RUN chmod +x deployment/*.sh

# Create dagster_home directory structure
RUN mkdir -p ${DAGSTER_HOME}/storage ${DAGSTER_HOME}/local ${DAGSTER_HOME}/logs

# Pre-install dbt packages
RUN cd dbt_project && dbt deps --quiet --profiles-dir .dbt || true && cd ..

EXPOSE 3000
