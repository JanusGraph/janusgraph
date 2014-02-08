package com.thinkaurelius.titan.diskstorage.log;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLogManager;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class KCVSLogTest extends LogTest {

    public abstract KeyColumnValueStoreManager openStorageManager() throws StorageException;

    private KeyColumnValueStoreManager storeManager;

    @Override
    public LogManager openLogManager(String senderId) throws StorageException {
        storeManager = openStorageManager();
        return new KCVSLogManager(storeManager,senderId, Configuration.EMPTY);
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
