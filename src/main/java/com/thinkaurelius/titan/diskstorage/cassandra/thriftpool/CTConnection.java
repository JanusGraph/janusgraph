package com.thinkaurelius.titan.diskstorage.cassandra.thriftpool;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

/**
 * Wraps a Cassandra.Client object along with its Thrift protocol and transport
 * objects.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CTConnection {
	private final TTransport transport;
	private final TProtocol proto;
	private final Cassandra.Client client;
	
	public CTConnection(TTransport transport,
			TProtocol proto, Client client) {
		super();
		this.transport = transport;
		this.proto = proto;
		this.client = client;
	}

	public TTransport getTransport() {
		return transport;
	}

	public TProtocol getProto() {
		return proto;
	}

	public Cassandra.Client getClient() {
		return client;
	}

	@Override
	public String toString() {
		return "CTConnection [transport=" + transport + ", proto=" + proto
				+ ", client=" + client + "]";
	}
	
}