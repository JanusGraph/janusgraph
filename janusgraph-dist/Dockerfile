# Copyright 2019 JanusGraph Authors
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
