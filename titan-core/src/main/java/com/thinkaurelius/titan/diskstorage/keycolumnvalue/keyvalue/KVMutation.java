package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.Mutation;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

import java.util.List;

/**
 * {@link Mutation} for {@link KeyValueStore}.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class KVMutation extends Mutation<KeyValueEntry,StaticBuffer> {

    public KVMutation(List<KeyValueEntry> additions, List<StaticBuffer> deletions) {
        super(additions, deletions);
    }

    public KVMutation() {
        super();
    }

}
