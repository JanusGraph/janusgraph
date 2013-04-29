package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.Mutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * {@link Mutation} for {@link KeyValueStore}.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class KVMutation extends Mutation<KeyValueEntry,ByteBuffer> {

    public KVMutation(List<KeyValueEntry> additions, List<ByteBuffer> deletions) {
        super(additions, deletions);
    }

    public KVMutation() {
        super();
    }

}
