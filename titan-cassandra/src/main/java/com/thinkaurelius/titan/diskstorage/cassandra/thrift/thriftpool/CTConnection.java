package com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.transport.TTransport;

import java.io.Closeable;

/**
 * Wraps a {@code Cassandra.Client} instance, its underlying {@code TTransport}
 * instance, and the {@link CTConnectionFactory.Config} instance used to setup
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