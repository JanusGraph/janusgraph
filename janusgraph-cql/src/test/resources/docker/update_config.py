#!/usr/bin/python
#
# Copyright 2017 JanusGraph Authors
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

import os,re

keystore = '/etc/ssl/node.keystore'
keystore_password = 'cassandra'
truststore = '/etc/ssl/node.truststore'
truststore_password = 'cassandra'
enableSsl = os.environ.get('CASSANDRA_ENABLE_SSL','').lower() == 'true'
enableClientAuth = os.environ.get('CASSANDRA_ENABLE_CLIENT_AUTH').lower() == 'true'
enableBop = os.environ.get('CASSANDRA_ENABLE_BOP','').lower() == 'true'

filename = '/etc/cassandra/cassandra.yaml'
with open(filename,'r') as f:
    s = f.read()

if enableSsl:
    ssl_conf = re.search('(client_encryption_options:.*?)[\n\r]{2}',s,re.M|re.DOTALL).group(1)
    ssl_conf_new = re.sub('enabled:.*','enabled: true', ssl_conf)
    ssl_conf_new = re.sub('keystore:.*','keystore: %s' % keystore, ssl_conf_new)
    ssl_conf_new = re.sub('keystore_password:.*','keystore_password: %s' % keystore_password, ssl_conf_new)
    s = s.replace(ssl_conf, ssl_conf_new)

if enableClientAuth:
    ssl_conf = re.search('(client_encryption_options:.*?)[\n\r]{2}',s,re.M|re.DOTALL).group(1)
    ssl_conf_new = re.sub('enabled:.*','enabled: true', ssl_conf)
    ssl_conf_new = re.sub('keystore:.*','keystore: %s' % keystore, ssl_conf_new)
    ssl_conf_new = re.sub('keystore_password:.*','keystore_password: %s' % keystore_password, ssl_conf_new)
    ssl_conf_new = re.sub('require_client_auth:.*','require_client_auth: true', ssl_conf_new)
    ssl_conf_new = re.sub('truststore:.*','truststore: %s' % truststore, ssl_conf_new)
    ssl_conf_new = re.sub('truststore_password:.*','truststore_password: %s' % truststore_password, ssl_conf_new)
    s = s.replace(ssl_conf, ssl_conf_new)

if enableBop:
    s = re.sub('partitioner:.*', 'partitioner: org.apache.cassandra.dht.ByteOrderedPartitioner', s)
    s = re.sub('# initial_token:.*', 'initial_token: 0000000000000000000000000000000000', s)
    s = re.sub('num_tokens:','#num_tokens:', s)
else:
    s = re.sub('num_tokens:.*','num_tokens: 4', s)

with open(filename,'w') as f:
    f.write(s)
