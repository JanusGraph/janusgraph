package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Features;

/**
 * Blueprint's features of a TitanGraph.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TitanFeatures {

    private static Features getBaselineTitanFeatures() {
        Features features = new Features();
        features.supportsDuplicateEdges = true;
        features.supportsSelfLoops = true;
        features.supportsSerializableObjectProperty = true;
        features.supportsBooleanProperty = true;
        features.supportsDoubleProperty = true;
        features.supportsFloatProperty = true;
        features.supportsIntegerProperty = true;
        features.supportsPrimitiveArrayProperty = true;
        features.supportsUniformListProperty = true;
        features.supportsMixedListProperty = true;
        features.supportsLongProperty = true;
        features.supportsMapProperty = true;
        features.supportsStringProperty = true;
        features.ignoresSuppliedIds = true;
        features.isPersistent = true;
        features.isWrapper = false;
        features.supportsIndices = false;
        features.supportsVertexIndex = false;
        features.supportsEdgeIndex = false;
        features.supportsKeyIndices = true;
        features.supportsVertexKeyIndex = true;
        features.supportsEdgeKeyIndex = true;
        features.supportsEdgeIteration = false;
        features.supportsVertexIteration = false;
        features.supportsVertexProperties = true;
        features.supportsEdgeProperties = true;
        features.supportsEdgeRetrieval = true;
        features.supportsTransactions = true;
        features.supportsThreadedTransactions = true;
        features.checkCompliance();
        return features;
    }

    public static Features getFeatures(GraphDatabaseConfiguration config, StoreFeatures storageFeatures) {
        Features features = TitanFeatures.getBaselineTitanFeatures();
        features.supportsSerializableObjectProperty = config.hasSerializeAll();
        if (storageFeatures != null) {
            if (storageFeatures.supportsScan()) {
                features.supportsVertexIteration = true;
                features.supportsEdgeIteration = true;
            }
        }
        return features;
    }

}
