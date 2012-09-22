package com.thinkaurelius.faunus.formats.titan.cassandra;

import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.titan.FaunusVertexRelationLoader;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import org.apache.cassandra.db.IColumn;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class FaunusTitanCassandraGraph extends StandardTitanGraph {

    private final InternalTitanTransaction tx;

    public FaunusTitanCassandraGraph(final String configFile) throws ConfigurationException {
        this(new PropertiesConfiguration(configFile));
    }

    public FaunusTitanCassandraGraph(final Configuration configuration) {
        super(new GraphDatabaseConfiguration(configuration));
        this.tx = startTransaction(new TransactionConfig(this.getConfiguration()));
    }

    public FaunusVertex readFaunusVertex(final ByteBuffer key, final SortedMap<ByteBuffer, IColumn> value) {
        FaunusVertexRelationLoader loader = new FaunusVertexRelationLoader(key);
        loadRelations(new CassandraMapIterable(value), loader, tx);
        return loader.getVertex();
    }

    @Override
    public void shutdown() {
        tx.abort();
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

