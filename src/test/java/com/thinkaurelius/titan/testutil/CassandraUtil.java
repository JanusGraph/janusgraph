package com.thinkaurelius.titan.testutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.thinkaurelius.titan.diskstorage.astyanax.AstyanaxStorageManager;

public class CassandraUtil {
	
	private static final Logger log = LoggerFactory.getLogger(CassandraUtil.class);

	public static void dropKeyspace(String name, String hostname, int port) {
		AstyanaxContext<Cluster> ctx = new AstyanaxContext.Builder()
				.forCluster(AstyanaxStorageManager.CLUSTER_DEFAULT)
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl()
								.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl("MyConnectionPool")
								.setPort(port).setMaxConnsPerHost(1)
								.setSeeds(hostname))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildCluster(ThriftFamilyFactory.getInstance());

		ctx.start();
		try {
			ctx.getEntity().dropKeyspace(name);
		} catch (ConnectionException e) {
			log.debug("Failed to drop keyspace {}", name);
		}
		ctx.shutdown();
	}
	
	public static void dropKeyspace(String name) {
		dropKeyspace(name, "localhost", 9160);
	}
}
