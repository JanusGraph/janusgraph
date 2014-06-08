package com.thinkaurelius.titan.diskstorage.configuration;

import com.thinkaurelius.titan.diskstorage.util.time.Timestamps;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.backend.KCVSConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.diskstorage.util.StandardBaseTransactionConfig;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVSConfigTest extends WritableConfigurationTest {

    @Override
    public WriteConfiguration getConfig() {
        final KeyColumnValueStoreManager manager = new InMemoryStoreManager(Configuration.EMPTY);
        try {
            return new KCVSConfiguration(new BackendOperation.TransactionalProvider() {
                @Override
                public StoreTransaction openTx() throws StorageException {
                    return manager.beginTransaction(StandardBaseTransactionConfig.of(Timestamps.MICRO, manager.getFeatures().getKeyConsistentTxConfig()));
                }

                @Override
                public void close() throws StorageException {
                    manager.close();
                }
            }, Timestamps.MICRO,manager.openDatabase("titan"),"general");
        } catch (StorageException e) {
            throw new RuntimeException(e);
        }
    }
}
