package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import org.apache.cassandra.db.IColumn;
import org.apache.commons.configuration.Configuration;

import java.nio.ByteBuffer;
import java.util.SortedMap;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class FaunusTitanGraph extends StandardTitanGraph {

    private final StandardTitanTx tx; /* it's only for reading a Titan graph into Hadoop. */

    public FaunusTitanGraph(final Configuration configuration) {
        this(configuration, true);
    }

    public FaunusTitanGraph(final Configuration configuration, boolean autoTx) {
        super(new GraphDatabaseConfiguration(configuration));
        this.tx = (autoTx) ? newTransaction(new TransactionConfig(this.getConfiguration(), false)) : null;
    }

    protected FaunusVertex readFaunusVertex(final ByteBuffer key, Iterable<Entry> entries) {
        FaunusVertexLoader loader = new FaunusVertexLoader(key);
        for (Entry data : entries) {
            try {
                FaunusVertexLoader.RelationFactory factory = loader.getFactory();
                super.edgeSerializer.readRelation(factory,data,tx);
                factory.build();
            } catch (Exception e) {
                //Log exception
            }

        }
        return loader.getVertex();
    }

    @Override
    public void shutdown() {
        if (tx != null)
            tx.rollback();

        super.shutdown();
    }

}
