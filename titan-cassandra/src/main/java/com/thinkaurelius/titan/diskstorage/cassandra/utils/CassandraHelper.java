package com.thinkaurelius.titan.diskstorage.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;

public class CassandraHelper {
    /**
     * Orders first argument according to key positions in second argument.
     *
     * We need this to ensure that ordering of entries in the result would match ordering of keys,
     * as keys are token sorted in Cassandra.
     *
     * @param toOrder Result of the "multiget_slice" call key => entry(column,value); (potentially ordered differently).
     * @param orderedKeys Keys in the correct order.
     *
     *
     * @return Ordered list of entries.
     */
    public static List<List<Entry>> order(Map<ByteBuffer, List<Entry>> toOrder, List<StaticBuffer> orderedKeys) {
        List<List<Entry>> results = new ArrayList<List<Entry>>();

        // We need this to ensure that ordering of entries in the result would match ordering of keys,
        // as keys are token sorted in Cassandra.
        for (StaticBuffer key : orderedKeys) {
            results.add(toOrder.get(key.asByteBuffer()));
        }

        return results;
    }
}
