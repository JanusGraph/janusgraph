# Copyright 2021 JanusGraph Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM debian:bookworm-20240211 as builder

ARG HBASE_VERSION=2.6.0
ARG HBASE_DIST="https://archive.apache.org/dist/hbase"

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y curl

RUN curl -SL ${HBASE_DIST}/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz | tar -x -z && mv hbase-${HBASE_VERSION} /opt/hbase
WORKDIR /opt/hbase
RUN rm -rf ./docs ./LEGAL ./*.txt ./*/*.cmd

COPY ./hbase-site.xml /opt/hbase/conf/hbase-site.xml
COPY ./hbase-policy.xml /opt/hbase/conf/hbase-policy.xml

FROM openjdk:8-jre-bullseye

ARG HBASE_UID=1000
ARG HBASE_GID=1000

ENV LOGS_DIR=/data/logs

COPY --from=builder /opt/hbase/ /opt/hbase/

RUN ln -sf /opt/hbase/bin/* /usr/bin/
RUN mkdir -p $LOGS_DIR /data/hbase /data/wal /data/run /opt/hbase/logs
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y socat
WORKDIR /opt/hbase
COPY ./entrypoint.sh /entrypoint.sh

EXPOSE 2181
EXPOSE 16000
EXPOSE 16010
EXPOSE 16020
EXPOSE 16030

ENTRYPOINT ["/entrypoint.sh"]

RUN groupadd -g ${HBASE_GID} hbase && \
    useradd -s /bin/bash hbase -u ${HBASE_UID} -g ${HBASE_GID} && \
    chmod +x /entrypoint.sh && \
    chown -R ${HBASE_UID}:${HBASE_GID} /data && \
    chown -R ${HBASE_UID}:${HBASE_GID} /opt/hbase/logs
USER hbase
