package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Mutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransactionHandle;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface BufferMutationKeyColumnValueStore {
    
    public void mutateMany(Map<String,Map<ByteBuffer,Mutation>> mutations, StoreTransactionHandle txh) throws StorageException;
    
}
