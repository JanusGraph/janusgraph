package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface BufferMutationKeyColumnValueStore {

    public void mutateMany(Map<String, Map<ByteBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException;

}
