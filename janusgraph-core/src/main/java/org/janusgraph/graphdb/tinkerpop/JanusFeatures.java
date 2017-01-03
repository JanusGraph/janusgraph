package org.janusgraph.graphdb.tinkerpop;

import org.janusgraph.core.JanusTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusTx;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Blueprint's features of a JanusGraph.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class JanusFeatures implements Graph.Features {

    private static final Logger log =
            LoggerFactory.getLogger(JanusFeatures.class);


    private final GraphFeatures graphFeatures;
    private final VertexFeatures vertexFeatures;
    private final EdgeFeatures edgeFeatures;

    private final StandardJanusGraph graph;

    private JanusFeatures(StandardJanusGraph graph, StoreFeatures storageFeatures) {
        graphFeatures = new JanusGraphFeatures(storageFeatures.supportsPersistence());
        vertexFeatures = new JanusVertexFeatures();
        edgeFeatures = new JanusEdgeFeatures();
        this.graph = graph;
    }

    @Override
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }

    public static JanusFeatures getFeatures(StandardJanusGraph graph, StoreFeatures storageFeatures) {
        return new JanusFeatures(graph,storageFeatures);
    }

    private static class JanusDataTypeFeatures implements DataTypeFeatures {

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }
    }

    private static class JanusVariableFeatures extends JanusDataTypeFeatures implements VariableFeatures { }

    private static class JanusGraphFeatures extends JanusDataTypeFeatures implements GraphFeatures {

        private final boolean persists;

        private JanusGraphFeatures(boolean persists) {
            this.persists = persists;
        }

        @Override
        public VariableFeatures variables() {
            return new JanusVariableFeatures();
        }

        @Override
        public boolean supportsComputer() {
            return true;
        }

        @Override
        public boolean supportsPersistence() {
            return persists;
        }

        @Override
        public boolean supportsTransactions() {
            return true;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return true;
        }
    }

    private static class JanusVertexPropertyFeatures extends JanusDataTypeFeatures implements VertexPropertyFeatures {

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() { return false; }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }
    }

    private static class JanusEdgePropertyFeatures extends JanusDataTypeFeatures implements EdgePropertyFeatures {

    }

    private class JanusVertexFeatures implements VertexFeatures {

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            StandardJanusTx tx = (StandardJanusTx)JanusFeatures.this.graph.newTransaction();
            try {
                if (!tx.containsPropertyKey(key)) return tx.getConfiguration().getAutoSchemaMaker().defaultPropertyCardinality(key).convert();
                return tx.getPropertyKey(key).cardinality().convert();
            } finally {
                tx.rollback();
            }
        }

        @Override
        public VertexPropertyFeatures properties() {
            return new JanusVertexPropertyFeatures();
        }

        @Override
        public boolean supportsNumericIds()
        {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return JanusFeatures.this.graph.getConfiguration().allowVertexIdSetting();
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds()
        {
            return false;
        }

        @Override
        public boolean supportsCustomIds()
        {
            return false;
        }
    }

    private static class JanusEdgeFeatures implements EdgeFeatures {
        @Override
        public EdgePropertyFeatures properties() {
            return new JanusEdgePropertyFeatures();
        }

        @Override
        public boolean supportsCustomIds()
        {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() { return false; }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds()
        {
            return false;
        }
    }

}
