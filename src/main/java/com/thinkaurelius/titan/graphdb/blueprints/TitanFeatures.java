package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.diskstorage.StorageFeatures;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Features;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
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
        features.supportsEdgeRetrieval = true;
        features.supportsTransactions = true;
        features.supportsThreadedTransactions = true;
        features.checkCompliance();
        return features;
    }
    
    public static Features getFeatures(GraphDatabaseConfiguration config, StorageFeatures storageFeatures) {
        Features features = TitanFeatures.getBaselineTitanFeatures();
        features.supportsSerializableObjectProperty = config.hasSerializeAll();
        if (storageFeatures!=null) {
            if (storageFeatures.supportsScan()) {
                features.supportsVertexIteration=true;
                features.supportsEdgeIteration=true;
            }
        }
        return features;
    }
    
    public static Features getInMemoryFeatures() {
        Features features = TitanFeatures.getBaselineTitanFeatures();
        features.isPersistent = false;
        features.supportsVertexIteration = true;
        features.supportsEdgeIteration = true;
        features.supportsTransactions = false;
        features.supportsThreadedTransactions = false;
        return features;
    }
    
}
