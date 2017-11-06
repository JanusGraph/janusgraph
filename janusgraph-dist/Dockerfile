FROM openjdk:8

ARG SERVER_BASE

ENV JANUS_STORAGE_BACKEND=inmemory \
    JANUS_STORAGE_BATCH_LOADING=false \
    JANUS_STORAGE_BUFFER_SIZE=1024 \
    JANUS_STORAGE_CONNECTION_TIMEOUT="10000" \
    JANUS_STORAGE_HOSTNAME="" \
    JANUS_STORAGE_PAGE_SIZE=100 \
    JANUS_STORAGE_PARALLEL_BACKEND_OPS=true \
    JANUS_STORAGE_PASSWORD="" \
    JANUS_STORAGE_PORT="" \
    JANUS_STORAGE_READ_ONLY=false \
    JANUS_STORAGE_READ_TIME="10000" \
    JANUS_STORAGE_SETUP_WAIT="60000" \
    JANUS_STORAGE_TRANSACTIONS=true \
    JANUS_STORAGE_USERNAME="" \
    JANUS_STORAGE_WRITE_TIME="100000" \
    JANUS_BIND_HOST=0.0.0.0 \
    JANUS_BIND_PORT=8182 \
    JANUS_WORKER_POOL_SIZE=1 \
    JANUS_GREMLIN_POOL_SIZE=8

ADD src/docker/gremlin-server.yaml /opt/gremlin-server.yaml
ADD src/docker/janusgraph.properties /opt/janusgraph-template.properties
ADD src/docker/docker-entrypoint.sh /opt/docker-entrypoint.sh
ADD src/docker/append-util.sh /opt/append-util.sh
ADD src/docker/append-storage.sh /opt/append-storage.sh
ADD src/docker/append-index.sh /opt/append-index.sh
ADD janusgraph-dist-hadoop-2/target/${SERVER_BASE}.zip /opt/${SERVER_BASE}.zip

RUN apt-get update && \
    apt-get install -y unzip && \
    unzip /opt/${SERVER_BASE}.zip -d /opt && \
    mv /opt/${SERVER_BASE} /opt/janusgraph && \
    cd /opt && \
    chmod u+x docker-entrypoint.sh append-index.sh append-storage.sh append-util.sh && \
    apt-get remove -y unzip && \
    apt-get clean

EXPOSE 8182

ENTRYPOINT [ "/opt/docker-entrypoint.sh" ]
CMD [ "janus-server" ]
