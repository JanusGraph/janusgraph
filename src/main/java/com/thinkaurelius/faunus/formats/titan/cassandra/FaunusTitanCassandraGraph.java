package com.thinkaurelius.faunus.formats.titan.cassandra;

import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.titan.FaunusTitanGraph;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import org.apache.cassandra.db.IColumn;
import org.apache.commons.configuration.Configuration;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class FaunusTitanCassandraGraph extends FaunusTitanGraph {

    public FaunusTitanCassandraGraph(final Configuration configuration) {
        super(configuration);
    }

    public FaunusTitanCassandraGraph(final Configuration configuration, boolean autoTx) {
        super(configuration, autoTx);
    }

    public FaunusVertex readFaunusVertex(final ByteBuffer key, final SortedMap<ByteBuffer, IColumn> value) {
        return super.readFaunusVertex(key, new CassandraMapIterable(value));
    }

    private static class CassandraMapIterable implements Iterable<Entry> {

        private final SortedMap<ByteBuffer, IColumn> columnValues;

        public CassandraMapIterable(final SortedMap<ByteBuffer, IColumn> columnValues) {
            Preconditions.checkNotNull(columnValues);
            this.columnValues = columnValues;
        }

        @Override
        public Iterator<Entry> iterator() {
            return new CassandraMapIterator(columnValues.entrySet().iterator());
        }

    }

    private static class CassandraMapIterator implements Iterator<Entry> {

        private final Iterator<Map.Entry<ByteBuffer, IColumn>> iterator;

        public CassandraMapIterator(final Iterator<Map.Entry<ByteBuffer, IColumn>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry next() {
            final Map.Entry<ByteBuffer, IColumn> entry = iterator.next();
            return new Entry(entry.getKey(), entry.getValue().value());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}

