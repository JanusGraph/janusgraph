package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import org.apache.commons.configuration.Configuration;

import java.nio.ByteBuffer;

/**
 * The backend agnostic Titan graph reader for pulling a graph of Titan and into Faunus.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (marko@markorodriguez.com)
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
        final FaunusVertexLoader loader = new FaunusVertexLoader(new StaticByteBuffer(key));
        for (final Entry data : entries) {
            try {
                final FaunusVertexLoader.RelationFactory factory = loader.getFactory();
                super.edgeSerializer.readRelation(factory, data, tx);
                factory.build();
            } catch (Exception e) {
                //  TODO: log exception
            }
        }
        return loader.getVertex();
    }

    @Override
    public void shutdown() {
        if (this.tx != null)
            this.tx.rollback();
        super.shutdown();
    }

}
