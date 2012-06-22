package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Features;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TitanFeatures {
    
    public static Features getBaselineTitanFeatures() {
        Features features = new Features();
        features.supportsDuplicateEdges = true;
        features.supportsSelfLoops = true;
        features.supportsSerializableObjectProperty = true; //TODO: this is configurable!
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
        features.isRDFModel = false;
        features.isWrapper = false;
        features.supportsIndices = false;
        features.supportsVertexIndex = false;
        features.supportsEdgeIndex = false;
        features.supportsKeyIndices = true;
        features.supportsVertexKeyIndex = true;
        features.supportsEdgeKeyIndex = false;
        features.supportsEdgeIteration = false;
        features.supportsVertexIteration = false;
        features.supportsVertexProperties = true;
        features.supportsEdgeProperties = true;
        features.supportsEdgeRetrieval = false;
        features.supportsTransactions = true;
        features.supportsThreadedTransactions = true;
        features.checkCompliance();
        return features;
    }
    
}
