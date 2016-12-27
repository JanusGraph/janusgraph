package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Blueprint's features of a TitanGraph.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TitanFeatures implements Graph.Features {

    private static final Logger log =
            LoggerFactory.getLogger(TitanFeatures.class);


    private final GraphFeatures graphFeatures;
    private final VertexFeatures vertexFeatures;
    private final EdgeFeatures edgeFeatures;

    private final StandardTitanGraph graph;

    private TitanFeatures(StandardTitanGraph graph, StoreFeatures storageFeatures) {
        graphFeatures = new TitanGraphFeatures(storageFeatures.supportsPersistence());
        vertexFeatures = new TitanVertexFeatures();
        edgeFeatures = new TitanEdgeFeatures();
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

    public static TitanFeatures getFeatures(StandardTitanGraph graph, StoreFeatures storageFeatures) {
        return new TitanFeatures(graph,storageFeatures);
    }

    private static class TitanDataTypeFeatures implements DataTypeFeatures {

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

    private static class TitanVariableFeatures extends TitanDataTypeFeatures implements VariableFeatures { }

    private static class TitanGraphFeatures extends TitanDataTypeFeatures implements GraphFeatures {

        private final boolean persists;

        private TitanGraphFeatures(boolean persists) {
            this.persists = persists;
        }

        @Override
        public VariableFeatures variables() {
            return new TitanVariableFeatures();
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

    private static class TitanVertexPropertyFeatures extends TitanDataTypeFeatures implements VertexPropertyFeatures {

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

    private static class TitanEdgePropertyFeatures extends TitanDataTypeFeatures implements EdgePropertyFeatures {

    }

    private class TitanVertexFeatures implements VertexFeatures {

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            StandardTitanTx tx = (StandardTitanTx)TitanFeatures.this.graph.newTransaction();
            try {
                if (!tx.containsPropertyKey(key)) return tx.getConfiguration().getAutoSchemaMaker().defaultPropertyCardinality(key).convert();
                return tx.getPropertyKey(key).cardinality().convert();
            } finally {
                tx.rollback();
            }
        }

        @Override
        public VertexPropertyFeatures properties() {
            return new TitanVertexPropertyFeatures();
        }

        @Override
        public boolean supportsNumericIds()
        {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return TitanFeatures.this.graph.getConfiguration().allowVertexIdSetting();
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

    private static class TitanEdgeFeatures implements EdgeFeatures {
        @Override
        public EdgePropertyFeatures properties() {
            return new TitanEdgePropertyFeatures();
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
