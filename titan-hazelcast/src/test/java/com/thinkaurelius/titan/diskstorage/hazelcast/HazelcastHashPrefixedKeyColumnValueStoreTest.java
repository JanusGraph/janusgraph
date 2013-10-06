package com.thinkaurelius.titan.diskstorage.hazelcast;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.indexing.HashPrefixKeyColumnValueStore;

public class HazelcastHashPrefixedKeyColumnValueStoreTest extends HazelcastKeyColumnValueStoreTest {

    public HazelcastHashPrefixedKeyColumnValueStoreTest() throws StorageException {
        super();
        store = new HashPrefixKeyColumnValueStore(store, 4);
    }

}