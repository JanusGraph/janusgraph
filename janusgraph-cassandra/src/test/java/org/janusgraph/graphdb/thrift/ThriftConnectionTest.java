// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.thrift;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.net.InetAddress;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.thrift.ThriftServer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.janusgraph.diskstorage.cassandra.thrift.thriftpool.CTConnection;
import org.janusgraph.diskstorage.cassandra.thrift.thriftpool.CTConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThriftConnectionTest {

    private static final int FRAME_SIZE = 15 * 1024 * 1024;
    private static final int TIMEOUT_MS = 5 * 1000;
    private static final int BACKLOG_PORT = 9098;
    private static final int LISTEN_PORT = 9099;

    private final Logger log = LoggerFactory.getLogger(ThriftConnectionTest.class);
    private CTConnectionFactory.Config factoryConfig;

    @BeforeEach
    public void setUp() {
        try {
            Config.setClientMode(true);
            Schema.instance.load(KSMetaData.newKeyspace("janusgraph", "SimpleStrategy", ImmutableMap.of("replication_factor", "1"), true));
            
            log.debug("Starting ThriftServer for connection drop on port [{}]", LISTEN_PORT);
            final InetAddress localhost = InetAddress.getLoopbackAddress();
            final ThriftServer server = new ThriftServer(localhost, LISTEN_PORT, BACKLOG_PORT);
            server.start();
            log.debug("Started ThriftServer for connection drop on port [{}]", LISTEN_PORT);
            
            final String[] hosts = new String[] { localhost.getHostAddress() };
            factoryConfig = new CTConnectionFactory.Config(hosts, LISTEN_PORT, null, null)
                    .setTimeoutMS(TIMEOUT_MS)
                    .setFrameSize(FRAME_SIZE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConnectionDropped() throws Exception {
        CTConnectionFactory connectionFactory = spy(factoryConfig.build());
        CTConnection mockConnection = spy(connectionFactory.makeObject("janusgraph"));
        
        when(mockConnection.getConfig()).thenReturn(factoryConfig);
        when(mockConnection.isOpen()).thenReturn(true);
        TTransport mockClient = spy(mockConnection.getTransport());

        assertTrue(connectionFactory.validateObject(null, mockConnection));
        when(mockClient.readAll(new byte[0], 0,0)).thenThrow(new TTransportException("Broken Pipe"));
        assertTrue(mockClient.isOpen());
    }
}
