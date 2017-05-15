FROM openjdk:8

ARG server_zip
ADD ${server_zip} /var

RUN apt-get update -y && apt-get install -y zip && \
    server_base=`basename ${server_zip} .zip` && \
    unzip -q /var/${server_base}.zip -d /var && \
    ln -s /var/${server_base} /var/janusgraph

WORKDIR /var/janusgraph
