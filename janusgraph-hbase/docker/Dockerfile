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

FROM debian:buster as builder

ARG HBASE_VERSION=2.2.7
ARG HBASE_DIST="http://ftp.halifax.rwth-aachen.de/apache/hbase"

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends curl=7.64.0-4+deb10u2

RUN curl -SL ${HBASE_DIST}/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz | tar -x -z && mv hbase-${HBASE_VERSION} /opt/hbase
WORKDIR /opt/hbase
RUN rm -rf ./docs ./LEGAL ./*.txt ./*/*.cmd

COPY ./hbase-site.xml /opt/hbase/conf/hbase-site.xml
COPY ./hbase-policy.xml /opt/hbase/conf/hbase-policy.xml
COPY ./zoo.cfg /opt/hbase/conf/zoo.cfg

FROM openjdk:8-jre-buster

ARG HBASE_UID=1000
ARG HBASE_GID=1000

ENV LOGS_DIR=/data/logs

COPY --from=builder /opt/hbase/ /opt/hbase/

RUN ln -sf /opt/hbase/bin/* /usr/bin/
RUN mkdir -p $LOGS_DIR /data/hbase /data/run

WORKDIR /opt/hbase

EXPOSE 2181
EXPOSE 16000
EXPOSE 16010
EXPOSE 16020
EXPOSE 16030

CMD ["/usr/bin/hbase", "master", "start"]

RUN groupadd -g ${HBASE_GID} hbase && \
    useradd -s /bin/bash hbase -u ${HBASE_UID} -g ${HBASE_GID} && \
    chown -R ${HBASE_UID}:${HBASE_GID} /data
USER hbase
