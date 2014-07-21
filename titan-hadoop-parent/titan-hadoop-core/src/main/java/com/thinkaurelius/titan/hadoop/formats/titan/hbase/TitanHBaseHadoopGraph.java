package com.thinkaurelius.titan.hadoop.formats.titan.hbase;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.formats.titan.TitanHadoopGraph;
import com.thinkaurelius.titan.hadoop.formats.titan.input.TitanHadoopSetup;

import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TitanHBaseHadoopGraph extends TitanHadoopGraph {

    public TitanHBaseHadoopGraph(TitanHadoopSetup setup) {
        super(setup);
    }

    public FaunusVertex readHadoopVertex(final Configuration configuration, byte[] key, final NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap) {
        return super.readHadoopVertex(configuration, new StaticArrayBuffer(key), new HBaseMapIterable(rowMap));
    }

    private static class HBaseMapIterable implements Iterable<Entry> {

        private final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnValues;

        public HBaseMapIterable(final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnValues) {
            Preconditions.checkNotNull(columnValues);
            this.columnValues = columnValues;
        }

        @Override
        public Iterator<Entry> iterator() {
            return new HBaseMapIterator(columnValues.entrySet().iterator());
        }

    }

    private static class HBaseMapIterator implements Iterator<Entry> {

        private final Iterator<Map.Entry<byte[], NavigableMap<Long, byte[]>>> iterator;

        public HBaseMapIterator(final Iterator<Map.Entry<byte[], NavigableMap<Long, byte[]>>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry next() {
            final Map.Entry<byte[], NavigableMap<Long, byte[]>> entry = iterator.next();
            byte[] col = entry.getKey();
            byte[] val = entry.getValue().lastEntry().getValue();
            return StaticArrayEntry.of(new StaticArrayBuffer(col), new StaticArrayBuffer(val));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}

