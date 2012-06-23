package com.thinkaurelius.titan.diskstorage.astyanax;

import org.apache.commons.configuration.Configuration;
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
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraLocalhostHelper;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class ExternalAstyanaxLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {
	
	private static Cluster cluster;
	
	@BeforeClass
	public static void connectToClusterForCleanup() {
		AstyanaxContext<Cluster> ctx = new AstyanaxContext.Builder()
				.forCluster(AstyanaxStorageManager.CLUSTER_NAME)
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
    public StorageManager openStorageManager(short idx) {
    	Configuration sc = CassandraLocalhostHelper.getLocalStorageConfiguration();
    	sc.addProperty(CassandraThriftStorageManager.LOCAL_LOCK_MEDIATOR_PREFIX_KEY, "astyanax-" + idx);
    	sc.addProperty(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY, idx);
    	
        return new AstyanaxStorageManager(sc);
    }

	@Override
	public void cleanUp() {
		try {
			cluster.dropKeyspace(AstyanaxStorageManager.KS_NAME);
			AstyanaxStorageManager.clearKeyspaces();
		} catch (ConnectionException e) {
//			throw new RuntimeException(e);
		}
	}
}
