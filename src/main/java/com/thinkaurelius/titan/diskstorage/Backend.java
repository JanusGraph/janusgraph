package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class Backend {

    //1. Store

    public EdgeStore getEdgeStore() throws StorageException;

    public KeyColumnValueStore getIndexStore() throws StorageException;

    //2. Entity Index

    //3. Messaging queues

    public BackendTransactionHandle beginTransaction() throws StorageException;

    public void close() throws StorageException;

}
