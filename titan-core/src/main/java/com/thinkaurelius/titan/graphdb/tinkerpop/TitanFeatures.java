package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

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


    private TitanFeatures() {
        graphFeatures = new TitanGraphFeatures();
        vertexFeatures = new TitanVertexFeatures();
        edgeFeatures = new TitanEdgeFeatures();
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


    private static final TitanFeatures INSTANCE = new TitanFeatures();


    public static TitanFeatures getFeatures(GraphDatabaseConfiguration config, StoreFeatures storageFeatures) {
        return INSTANCE;
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
            return true;
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

    private static class TitanVertexFeatures implements VertexFeatures {

        @Override
        public VertexPropertyFeatures properties() {
            return new TitanVertexPropertyFeatures();
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }
    }

    private static class TitanEdgeFeatures implements EdgeFeatures {
        @Override
        public EdgePropertyFeatures properties() {
            return new TitanEdgePropertyFeatures();
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
    }

}
