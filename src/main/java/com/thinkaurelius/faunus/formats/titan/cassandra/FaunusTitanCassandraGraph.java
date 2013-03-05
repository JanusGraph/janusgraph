package com.thinkaurelius.faunus.formats.titan.cassandra;

import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.titan.FaunusVertexLoader;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import org.apache.cassandra.db.IColumn;
import org.apache.commons.configuration.Configuration;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class FaunusTitanCassandraGraph extends StandardTitanGraph {

    private final StandardTitanTx tx; /* it's only for reading a Titan graph into Hadoop. */

    public FaunusTitanCassandraGraph(final Configuration configuration) {
        this(configuration, true);
    }

    public FaunusTitanCassandraGraph(final Configuration configuration, boolean autoTx) {
        super(new GraphDatabaseConfiguration(configuration));
        this.tx = (autoTx) ? newTransaction(new TransactionConfig(this.getConfiguration(), false)) : null;
    }

    public FaunusVertex readFaunusVertex(final ByteBuffer key, final SortedMap<ByteBuffer, IColumn> value) {
        FaunusVertexLoader loader = new FaunusVertexLoader(key);
        Iterable<Entry> entries = new CassandraMapIterable(value);
        for (Entry data : entries) {
            FaunusVertexLoader.RelationFactory factory = loader.getFactory();
            super.edgeSerializer.readRelation(factory,data,tx);
            factory.build();
        }
        return loader.getVertex();
    }

    @Override
    public void shutdown() {
        if (tx != null)
            tx.rollback();

        super.shutdown();
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

