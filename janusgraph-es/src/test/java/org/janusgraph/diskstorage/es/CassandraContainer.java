package org.janusgraph.diskstorage.es;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import java.time.Duration;

import static org.janusgraph.CassandraStorageSetup.cleanKeyspaceName;
import static org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_KEYSPACE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class CassandraContainer extends GenericContainer {

    public static final Integer THRIFT_PORT = 9160;
    public static final Integer CQL_PORT = 9042;
    public static final String CASSANDRA_VERSION = "3.11.2";
    public static final String IMAGE = "cassandra";

    public CassandraContainer() {
        super(IMAGE + ":" + (System.getenv("CASSANDRA_VERSION") != null ? System.getenv("CASSANDRA_VERSION") : CASSANDRA_VERSION));
    }

    @Override
    protected void configure() {
        addExposedPort(THRIFT_PORT);
        addExposedPort(CQL_PORT);
        addEnv("CASSANDRA_START_RPC", "true");
        waitingFor(Wait.forListeningPort());
    }

    public ModifiableConfiguration getGenericConfiguration(String ks, String backend) {
        ModifiableConfiguration mc = CassandraStorageSetup.getGenericConfiguration(ks, backend);

        mc.set(STORAGE_HOSTS, new String[]{getContainerIpAddress()});
        mc.set(STORAGE_PORT, getMappedPort(THRIFT_PORT));
        return mc;
    }


    public ModifiableConfiguration getThriftModifiableConfiguration(String keyspace) {
        return getGenericConfiguration(keyspace, "cassandrathrift");
    }
}
