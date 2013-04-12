package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import org.apache.commons.configuration.Configuration;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * The backend agnostic Titan graph reader for pulling a graph of Titan and into Faunus.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (marko@markorodriguez.com)
 */

public class FaunusTitanGraph extends StandardTitanGraph {

    private final StandardTitanTx tx; /* it's only for reading a Titan graph into Hadoop. */
    private boolean loadOutEdges = true;
    private boolean loadInEdges = true;
    private boolean loadProperties = true;

    public FaunusTitanGraph(final Configuration configuration) {
        this(configuration, true);
    }

    public FaunusTitanGraph(final Configuration configuration, boolean autoTx) {
        super(new GraphDatabaseConfiguration(configuration));
        this.tx = (autoTx) ? newTransaction(new TransactionConfig(this.getConfiguration(), false)) : null;
        final List<String> components = Arrays.asList(configuration.getStringArray(TitanInputFormat.FAUNUS_GRAPH_INPUT_TITAN_COMPONENTS));
        if (components.size() > 0) {
            this.loadOutEdges = components.contains(TitanInputFormat.OUT_EDGES);
            this.loadInEdges = components.contains(TitanInputFormat.IN_EDGES);
            this.loadProperties = components.contains(TitanInputFormat.PROPERTIES);
        }
    }

    protected FaunusVertex readFaunusVertex(final ByteBuffer key, Iterable<Entry> entries) {
        final FaunusVertexLoader loader = new FaunusVertexLoader(key);
        for (final Entry data : entries) {
            try {
                final FaunusVertexLoader.RelationFactory factory = loader.getFactory();
                super.edgeSerializer.readRelation(factory, data, tx);
                factory.build(this.loadProperties, this.loadInEdges, this.loadOutEdges);
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
