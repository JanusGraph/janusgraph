package com.thinkaurelius.titan.diskstorage.log;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreManager;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLogManager;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Implemention of the {@link LogTest} for {@link KCVSLogManager} based log implementations.
 * This test only requires getting instances of {@link KeyColumnValueStoreManager}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class KCVSLogTest extends LogTest {

    public abstract KeyColumnValueStoreManager openStorageManager() throws BackendException;

    public static final String LOG_NAME = "testlog";

    private KeyColumnValueStoreManager storeManager;

    @Override
    public LogManager openLogManager(String senderId, boolean requiresOrderPreserving) throws BackendException {
        storeManager = openStorageManager();
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID,senderId);
        config.set(GraphDatabaseConfiguration.LOG_READ_INTERVAL, Duration.ofMillis(500L), LOG_NAME);
        //To ensure that the write order is preserved in reading, we need to ensure that all writes go to the same partition
        //otherwise readers will independently read from the partitions out-of-order by design to avoid having to synchronize
        config.set(KCVSLogManager.LOG_FIXED_PARTITION, requiresOrderPreserving, LOG_NAME);
        return new KCVSLogManager(storeManager,config.restrictTo(LOG_NAME));
    }

    @Override
    public void setup() throws Exception {
        StoreManager m = openStorageManager();
        m.clearStorage();
        m.close();
        super.setup();
    }

    @Override
    public void shutdown() throws Exception {
        super.shutdown();
        storeManager.close();
    }

}
