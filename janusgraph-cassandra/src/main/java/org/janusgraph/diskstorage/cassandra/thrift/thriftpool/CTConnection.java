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

package org.janusgraph.diskstorage.cassandra.thrift.thriftpool;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.transport.TTransport;

import java.io.Closeable;

/**
 * Wraps a {@code Cassandra.Client} instance, its underlying {@code TTransport}
 * instance, and the {@link org.janusgraph.diskstorage.cassandra.thrift.thriftpool.CTConnectionFactory.Config} instance used to setup
 * the connection.
 * 
 * @see CTConnectionFactory
 * @see CTConnectionPool
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CTConnection implements Closeable {
    
    private final TTransport transport;
    private final Cassandra.Client client;
    private final CTConnectionFactory.Config cfg;

    public CTConnection(TTransport transport, Client client, CTConnectionFactory.Config cfg) {
        this.transport = transport;
        this.client = client;
        this.cfg = cfg;
    }

    public TTransport getTransport() {
        return transport;
    }

    public Cassandra.Client getClient() {
        return client;
    }
    
    public CTConnectionFactory.Config getConfig() {
     return cfg;
    }

    public boolean isOpen() {
        return transport.isOpen();
    }
    @Override
    public void close() {
        if (transport != null && transport.isOpen())
            transport.close();
    }

    @Override
    public String toString() {
        return "CTConnection [transport=" + transport + ", client=" + client + ", cfg=" + cfg + "]";
    }
}
