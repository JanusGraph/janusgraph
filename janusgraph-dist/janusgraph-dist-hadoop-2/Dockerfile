FROM openjdk:8

ENV PATH "$JAVA_HOME/bin:$PATH"
RUN apt-get update -y && apt-get install -y zip
WORKDIR /var

ARG server_zip
ARG server_base
ADD ${server_zip} /var
RUN unzip -q ${server_base}.zip && ln -s ./${server_base} ./janusgraph
WORKDIR /var/janusgraph
EXPOSE 8182
CMD ["./bin/gremlin-server.sh", "./conf/gremlin-server/gremlin-server-berkeleyje.yaml"]
