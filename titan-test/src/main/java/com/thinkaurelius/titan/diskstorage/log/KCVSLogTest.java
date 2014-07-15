package com.thinkaurelius.titan.diskstorage.log;

import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreManager;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLogManager;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
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
    public LogManager openLogManager(String senderId) throws BackendException {
        storeManager = openStorageManager();
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildConfiguration();
        config.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID,senderId);
        config.set(GraphDatabaseConfiguration.LOG_READ_INTERVAL, new StandardDuration(500L, TimeUnit.MILLISECONDS), LOG_NAME);
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
