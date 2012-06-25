package com.thinkaurelius.titan.graphdb.astyanax;

import org.junit.BeforeClass;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.astyanax.AstyanaxStorageManager;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class ExternalAstyanaxGraphTest extends TitanGraphTest {

	private static Cluster cluster;
	
	public ExternalAstyanaxGraphTest() {
		super(StorageSetup.getAstyanaxGraphConfiguration());
	}
	
	@BeforeClass
	public static void connectToClusterForCleanup() {
		AstyanaxContext<Cluster> ctx = new AstyanaxContext.Builder()
				.forCluster(AstyanaxStorageManager.CLUSTER_DEFAULT)
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl()
								.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl("MyConnectionPool")
								.setPort(9160).setMaxConnsPerHost(16)
								.setSeeds("localhost"))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildCluster(ThriftFamilyFactory.getInstance());

		cluster = ctx.getEntity();
		ctx.start();
	}
	
	@Override
	public void cleanUp() {
		try {
			cluster.dropKeyspace(AstyanaxStorageManager.KEYSPACE_DEFAULT);
			AstyanaxStorageManager.clearKeyspaces();
		} catch (ConnectionException e) {
//			throw new RuntimeException(e);
		}
	}
}
