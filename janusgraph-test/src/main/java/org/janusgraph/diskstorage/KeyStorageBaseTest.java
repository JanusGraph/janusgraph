package org.janusgraph.diskstorage;

import org.janusgraph.JanusGraphDatabaseManager;
import org.janusgraph.JanusGraphDatabaseProvider;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public abstract class KeyStorageBaseTest {
    @Rule
    public TestName name = new TestName();

    protected JanusGraphDatabaseProvider graphProvider;

    @Before
    public void baseSetUp() {
        graphProvider = JanusGraphDatabaseManager.getGraphDatabaseProvider();
    }

    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return graphProvider.openStorageManager(this, name.getMethodName(), 0, null);
    }
    public KeyColumnValueStoreManager openStorageManager(int id, Configuration configuration) throws BackendException {
        return graphProvider.openStorageManager(this, name.getMethodName(), 0, null);
    }
    public OrderedKeyValueStoreManager openOrderedStorageManager() throws BackendException {
        return graphProvider.openOrderedStorageManager(this, name.getMethodName());
    }

}
