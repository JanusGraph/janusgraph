# Copyright 2023 JanusGraph Authors
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

ARG BASE_IMAGE=eclipse-temurin:11-jre

FROM debian:buster-slim as builder

ARG TARGETARCH
ARG JANUS_VERSION=1.0.0-SNAPSHOT
ARG BUILD_PATH=janusgraph-java-11-1.0.0-SNAPSHOT
ARG YQ_VERSION=3.4.1

ENV JANUS_VERSION=${JANUS_VERSION} \
    JANUS_HOME=/opt/janusgraph

WORKDIR /opt

COPY target/${BUILD_PATH}.zip /opt/${BUILD_PATH}.zip

RUN apt update -y && apt install -y gpg unzip curl && \
    curl -fSL https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/yq_linux_${TARGETARCH} -o yq && \
    unzip ${BUILD_PATH}.zip && \
    mv ${BUILD_PATH} /opt/janusgraph && \
    rm -rf ${JANUS_HOME}/elasticsearch && \
    rm -rf ${JANUS_HOME}/javadocs && \
    rm -rf ${JANUS_HOME}/log && \
    rm -rf ${JANUS_HOME}/examples && \
    rm -rf ${JANUS_HOME}/conf/janusgraph-*.properties && \
    mv ${JANUS_HOME}/conf/gremlin-server/gremlin-server.yaml ${JANUS_HOME}/conf/janusgraph-server.yaml && \
    rm -rf ${JANUS_HOME}/conf/gremlin-server

COPY docker/conf/ ${JANUS_HOME}/conf/
COPY docker/scripts/remote-connect.groovy ${JANUS_HOME}/scripts/

FROM ${BASE_IMAGE}
ARG JANUS_VERSION=1.0.0-SNAPSHOT

ENV JANUS_VERSION=${JANUS_VERSION} \
    JANUS_HOME=/opt/janusgraph \
    JANUS_CONFIG_DIR=/etc/opt/janusgraph \
    JANUS_DATA_DIR=/var/lib/janusgraph \
    JANUS_SERVER_TIMEOUT=30 \
    JANUS_STORAGE_TIMEOUT=60 \
    JANUS_PROPS_TEMPLATE=berkeleyje-lucene \
    JANUS_INITDB_DIR=/docker-entrypoint-initdb.d \
    gremlinserver.graphs.graph=/etc/opt/janusgraph/janusgraph.properties \
    gremlinserver.threadPoolWorker=1 \
    gremlinserver.gremlinPool=8

RUN groupadd -r janusgraph --gid=999 && \
    useradd -r -g janusgraph --uid=999 -d ${JANUS_DATA_DIR} janusgraph && \
    apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends krb5-user && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/janusgraph/ /opt/janusgraph/
COPY --from=builder /opt/yq /usr/bin/yq
COPY docker/docker-entrypoint.sh /usr/local/bin/
COPY docker/load-initdb.sh /usr/local/bin/

RUN chmod 755 /usr/local/bin/docker-entrypoint.sh && \
    chmod 755 /usr/local/bin/load-initdb.sh && \
    chmod 755 /usr/bin/yq && \
    mkdir -p ${JANUS_INITDB_DIR} ${JANUS_CONFIG_DIR} ${JANUS_DATA_DIR} && \
    chown -R janusgraph:janusgraph ${JANUS_HOME} ${JANUS_INITDB_DIR} ${JANUS_CONFIG_DIR} ${JANUS_DATA_DIR}

EXPOSE 8182

WORKDIR ${JANUS_HOME}
USER janusgraph

ENTRYPOINT [ "docker-entrypoint.sh" ]
CMD [ "janusgraph" ]

ARG CREATED=test
ARG REVISION=test

LABEL org.opencontainers.image.title="JanusGraph Docker Image" \
      org.opencontainers.image.description="Official JanusGraph Docker image" \
      org.opencontainers.image.url="https://janusgraph.org/" \
      org.opencontainers.image.documentation="https://docs.janusgraph.org/v1.0/" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.source="https://github.com/JanusGraph/janusgraph-docker/" \
      org.opencontainers.image.vendor="JanusGraph" \
      org.opencontainers.image.version="${JANUS_VERSION}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.license="Apache-2.0"
