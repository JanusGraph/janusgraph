package com.thinkaurelius.titan.diskstorage.log;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLogManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class KCVSLogTest extends LogTest {

    public abstract KeyColumnValueStoreManager openStorageManager() throws StorageException;

    public static final String LOG_NAME = "testlog";

    private KeyColumnValueStoreManager storeManager;

    @Override
    public LogManager openLogManager(String senderId) throws StorageException {
        storeManager = openStorageManager();
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildConfiguration();
        config.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID,senderId);
        return new KCVSLogManager(storeManager,config.restrictTo(LOG_NAME));
    }

    @Override
    public void setup() throws Exception {
        openStorageManager().clearStorage();
        super.setup();
    }

    @Override
    public void shutdown() throws Exception {
        super.shutdown();
        storeManager.close();
    }

}
