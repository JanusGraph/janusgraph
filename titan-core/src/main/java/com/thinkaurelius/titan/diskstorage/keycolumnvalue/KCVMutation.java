package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.Mutation;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class KCVMutation extends Mutation<Entry,ByteBuffer> {

    public KCVMutation(List<Entry> additions, List<ByteBuffer> deletions) {
        super(additions, deletions);
    }
}
