package com.thinkaurelius.faunus.formats.titan.hbase;

import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.titan.FaunusVertexLoader;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class FaunusTitanHBaseGraph extends StandardTitanGraph {

    private final StandardTitanTx tx; /* it's only for reading a Titan graph into Hadoop. */

    public FaunusTitanHBaseGraph(final String configFile) throws ConfigurationException {
        this(new PropertiesConfiguration(configFile));
    }

    public FaunusTitanHBaseGraph(final Configuration configuration) {
        super(new GraphDatabaseConfiguration(configuration));
        this.tx = newTransaction(new TransactionConfig(this.getConfiguration(),false));
    }

    public FaunusVertex readFaunusVertex(byte[] key, final NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap) {
        FaunusVertexLoader loader = new FaunusVertexLoader(ByteBuffer.wrap(key));
        Iterable<Entry> entries = new HBaseMapIterable(rowMap);
        for (Entry data : entries) {
            FaunusVertexLoader.RelationFactory factory = loader.getFactory();
            super.edgeSerializer.readRelation(factory,data,tx);
            factory.build();
        }
        return loader.getVertex();
    }

    @Override
    public void shutdown() {
        tx.rollback();
        super.shutdown();
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
            return new Entry(ByteBuffer.wrap(entry.getKey()), ByteBuffer.wrap(entry.getValue().lastEntry().getValue()));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}

