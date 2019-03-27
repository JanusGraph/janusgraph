FROM openjdk:8

ARG server_zip
ADD ${server_zip} /var

RUN apt-get update -y && apt-get install -y zip && \
    server_base=`basename ${server_zip} .zip` && \
    unzip -q /var/${server_base}.zip -d /var && \
    rm /var/${server_base}.zip && \
    ln -s /var/${server_base} /var/janusgraph && \
    groupadd -g 999 janusgraph && \
    useradd -d /home/janusgraph -m -r -u 999 -g janusgraph janusgraph && \
    chown -R janusgraph:janusgraph /var/${server_base} && \
    chmod 755 /var/${server_base} && \
    chown -R janusgraph:janusgraph /var/janusgraph && \
    chmod 755 /var/janusgraph

USER janusgraph

WORKDIR /var/janusgraph
