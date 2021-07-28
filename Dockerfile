FROM python:3.9.5-slim
COPY . /ethelease
WORKDIR /ethelease
ARG DEBIAN_FRONTEND=noninteractive
ENV CLOUD_SQL_PROXY_URL=https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64
RUN apt-get update \
    && apt-get install -y default-libmysqlclient-dev g++ gcc git libcurl4-openssl-dev libssl-dev \
    && apt-get install -y postgresql-client procps unixodbc-dev openssh-client wget \
    && pip install --upgrade pip \
    && pip install -r requirements.txt \
    && pip install . \
    && wget ${CLOUD_SQL_PROXY_URL} -O cloud_sql_proxy \
    && mv ./cloud_sql_proxy ../cloud_sql_proxy \
    && chmod +x ../cloud_sql_proxy \
    && apt-get purge -y --auto-remove g++ gcc git libssl-dev unixodbc-dev wget \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base
WORKDIR /
