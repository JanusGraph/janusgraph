package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StaticArrayEntryCache extends StaticArrayEntry {

    public StaticArrayEntryCache(byte[] array, int offset, int limit, int valuePosition) {
        super(array, offset, limit, valuePosition);
    }

    public StaticArrayEntryCache(byte[] array, int limit, int valuePosition) {
        super(array, limit, valuePosition);
    }

    public StaticArrayEntryCache(byte[] array, int valuePosition) {
        super(array, valuePosition);
    }

    public StaticArrayEntryCache(StaticArrayBuffer buffer, int valuePosition) {
        super(buffer, valuePosition);
    }



}
