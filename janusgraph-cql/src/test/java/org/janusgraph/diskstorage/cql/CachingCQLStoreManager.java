package org.janusgraph.diskstorage.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

public class CachingCQLStoreManager extends CQLStoreManager {

    private static Cluster cluster;
    private static Map<String,Session> sessions = new HashMap<>();

    public CachingCQLStoreManager(final Configuration configuration) throws BackendException {
        super(configuration);
    }

    @Override
    Cluster initializeCluster() throws PermanentBackendException {
        if (cluster == null || cluster.isClosed()) {
            cluster = super.initializeCluster();
        }
        return cluster;
    }

    @Override
    Session initializeSession(final String keyspaceName) {
        if (!sessions.containsKey(keyspaceName)) {
            sessions.put(keyspaceName, super.initializeSession(keyspaceName));
        }
        return sessions.get(keyspaceName);
    }

    @Override
    public void close() {
        this.executorService.shutdownNow();
    }

}
