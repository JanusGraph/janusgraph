package com.thinkaurelius.titan.diskstorage.infinispan;

import com.thinkaurelius.titan.InfinispanStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.CacheStoreManagerAdapter;

public abstract class AbstractInfinispanCacheAdapterTest extends KeyColumnValueStoreTest {

    private final boolean transactional;
    
    public AbstractInfinispanCacheAdapterTest(boolean transactional) throws StorageException {
        this.transactional = transactional;
        manager = openStorageManager();
        store = manager.openDatabase(storeName);
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager()
            throws StorageException {
        return new CacheStoreManagerAdapter(new InfinispanCacheStoreManager(
                InfinispanStorageSetup.getInfinispanBaseConfig(transactional)));
    }



    @Override
    public void setUp() throws StorageException {
        open();
    }

    @Override
    public void tearDown() throws Exception {
        close();
        manager.clearStorage();
    }

    @Override
    public void close() throws StorageException {
        if (tx != null)
            tx.commit();
    }

    @Override
    public void open() throws StorageException {
        tx = manager.beginTransaction(new StoreTxConfig());
    }
}
