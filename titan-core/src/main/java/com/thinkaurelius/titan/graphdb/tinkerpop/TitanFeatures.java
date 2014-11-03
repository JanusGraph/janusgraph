package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.StringFactory;
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
    private final VariableFeatures variableFeatures;
    private final VertexFeatures vertexFeatures;
    private final EdgeFeatures edgeFeatures;


    private TitanFeatures() {
        variableFeatures = new VariableFeatures() {
            @Override
            public boolean supportsVariables() {
                return true;
            }

            @Override
            public boolean supportsSerializableValues() { return false; }
        };
        graphFeatures = new GraphFeatures() {
            @Override
            public VariableFeatures variables() {
                return variableFeatures;
            }

            @Override
            public boolean supportsComputer() {
                return false; //TODO: set to true
            }
        };
        vertexFeatures = new VertexFeatures() {
            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public VertexPropertyFeatures properties() {
                return new VertexPropertyFeatures() {
                    @Override
                    public boolean supportsUserSuppliedIds() {
                        return false;
                    }

                    @Override
                    public boolean supportsSerializableValues() { return false; }
                };
            }

        };
        edgeFeatures = new EdgeFeatures() {
            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public EdgePropertyFeatures properties() {
                return new EdgePropertyFeatures() {
                    @Override
                    public boolean supportsSerializableValues() { return false; }
                };
            }

        };

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

}
