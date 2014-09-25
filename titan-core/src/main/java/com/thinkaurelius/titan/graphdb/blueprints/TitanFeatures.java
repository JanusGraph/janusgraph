package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Features;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * Blueprint's features of a TitanGraph.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TitanFeatures {

    private static final Logger log =
            LoggerFactory.getLogger(TitanFeatures.class);

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
        features.supportsVertexKeyIndex = false;
        features.supportsEdgeKeyIndex = false;
        features.supportsEdgeIteration = false;
        features.supportsVertexIteration = false;
        features.supportsVertexProperties = true;
        features.supportsEdgeProperties = true;
        features.supportsEdgeRetrieval = true;
        features.supportsTransactions = true;
        features.supportsThreadedTransactions = true;
        setIfExists(features, "supportsThreadIsolatedTransactions", true);
        features.checkCompliance();
        return features;
    }

    private static void setIfExists(Features features, String fieldName, boolean value) {

        final Class<?> requiredFieldType = Boolean.class;
        final Class<?> fc = features.getClass();
        try {
            Field f = fc.getField(fieldName);
            if (!requiredFieldType.equals(f.getType())) {
                log.debug("Field {}.{} has type {} (but type {} was expected)",
                        fc, fieldName, f.getType(), requiredFieldType);
                return;
            }
            f.set(features, value);
            log.debug("Set {}.{} to {}", fc, fieldName, value);
        } catch (NoSuchFieldException e) {
            log.debug("Field {}.{} could not be found", fc, fieldName, e);
        } catch (IllegalAccessException e) {
            log.warn("Unable to reflectively customize {}.{}", fc, fieldName, e);
        }
    }

    public static Features getFeatures(GraphDatabaseConfiguration config, StoreFeatures storageFeatures) {
        Features features = TitanFeatures.getBaselineTitanFeatures();
        features.supportsSerializableObjectProperty = config.hasSerializeAll();
        if (storageFeatures != null) {
            if (storageFeatures.hasScan()) {
                features.supportsVertexIteration = true;
                features.supportsEdgeIteration = true;
            }
        }
        return features;
    }

}
