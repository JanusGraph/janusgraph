package com.thinkaurelius.titan.hadoop.formats.cassandra;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.formats.util.TitanHadoopGraph;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup;

import org.apache.cassandra.db.Column;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TitanCassandraHadoopGraph extends TitanHadoopGraph {

    public TitanCassandraHadoopGraph(TitanHadoopSetup setup) {
        super(setup);
    }

    public FaunusVertex readHadoopVertex(final Configuration configuration, final ByteBuffer key, final SortedMap<ByteBuffer, Column> value) {
        return super.readHadoopVertex(configuration, StaticArrayBuffer.of(key), new CassandraMapIterable(value));
    }

    private static class CassandraMapIterable implements Iterable<Entry> {

        private final SortedMap<ByteBuffer, Column> columnValues;

        public CassandraMapIterable(final SortedMap<ByteBuffer, Column> columnValues) {
            Preconditions.checkNotNull(columnValues);
            this.columnValues = columnValues;
        }

        @Override
        public Iterator<Entry> iterator() {
            return new CassandraMapIterator(columnValues.entrySet().iterator());
        }

    }

    private static class CassandraMapIterator implements Iterator<Entry> {

        private final Iterator<Map.Entry<ByteBuffer, Column>> iterator;

        public CassandraMapIterator(final Iterator<Map.Entry<ByteBuffer, Column>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry next() {
            final Map.Entry<ByteBuffer, Column> entry = iterator.next();
            ByteBuffer col = entry.getKey();
            ByteBuffer val = entry.getValue().value();
            return StaticArrayEntry.of(StaticArrayBuffer.of(col), StaticArrayBuffer.of(val));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}

