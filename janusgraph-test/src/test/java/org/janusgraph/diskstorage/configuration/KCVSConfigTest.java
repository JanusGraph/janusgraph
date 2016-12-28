package org.janusgraph.diskstorage.configuration;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.diskstorage.configuration.backend.KCVSConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVSConfigTest extends WritableConfigurationTest {

    @Override
    public WriteConfiguration getConfig() {
        final KeyColumnValueStoreManager manager = new InMemoryStoreManager(Configuration.EMPTY);
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.TIMESTAMP_PROVIDER, TimestampProviders.MICRO);
        try {
            return new KCVSConfiguration(new BackendOperation.TransactionalProvider() {
                @Override
                public StoreTransaction openTx() throws BackendException {
                    return manager.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MICRO, manager.getFeatures().getKeyConsistentTxConfig()));
                }

                @Override
                public void close() throws BackendException {
                    manager.close();
                }
            }, config, manager.openDatabase("titan"),"general");
        } catch (BackendException e) {
            throw new RuntimeException(e);
        }
    }
}
