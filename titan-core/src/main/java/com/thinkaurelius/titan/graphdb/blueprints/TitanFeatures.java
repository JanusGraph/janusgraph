package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

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
                return false;
            }
        };
        graphFeatures = new GraphFeatures() {
            @Override
            public VariableFeatures variables() {
                return variableFeatures;
            }
        };
        vertexFeatures = new VertexFeatures() { //Default works
        };
        edgeFeatures = new EdgeFeatures() { //Default works
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

    private static final TitanFeatures INSTANCE = new TitanFeatures();


    public static TitanFeatures getFeatures(GraphDatabaseConfiguration config, StoreFeatures storageFeatures) {
        return INSTANCE;
    }

}
