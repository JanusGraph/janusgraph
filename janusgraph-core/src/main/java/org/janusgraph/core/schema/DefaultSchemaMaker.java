package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.VertexLabel;

/**
 * When a graph is configured to automatically create vertex/edge labels and property keys when they are first used,
 * a DefaultTypeMaker implementation is used to define them by invoking the {@link #makeVertexLabel(VertexLabelMaker)},
 * {@link #makeEdgeLabel(EdgeLabelMaker)}, or {@link #makePropertyKey(PropertyKeyMaker)} methods respectively.
 * <br />
 * By providing a custom DefaultTypeMaker implementation, one can specify how these types should be defined by default.
 * A DefaultTypeMaker implementation is specified in the graph configuration using the full path which means the
 * implementation must be on the classpath.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see RelationTypeMaker
 */
public interface DefaultSchemaMaker {

    /**
     * Creates a new edge label with default settings against the provided {@link EdgeLabelMaker}.
     *
     * @param factory EdgeLabelMaker through which the edge label is created
     * @return A new edge label
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    public default EdgeLabel makeEdgeLabel(EdgeLabelMaker factory) {
        return factory.directed().make();
    }

    /**
     *
     * @return the default cardinality of a property if created for the given key
     */
    public Cardinality defaultPropertyCardinality(String key);

    /**
     * Creates a new property key with default settings against the provided {@link PropertyKeyMaker}.
     *
     * @param factory PropertyKeyMaker through which the property key is created
     * @return A new property key
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    public default PropertyKey makePropertyKey(PropertyKeyMaker factory) {
        return factory.cardinality(defaultPropertyCardinality(factory.getName())).dataType(Object.class).make();
    }

    /**
     * Creates a new vertex label with the default settings against the provided {@link VertexLabelMaker}.
     *
     * @param factory VertexLabelMaker through which the vertex label is created
     * @return A new vertex label
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    public default VertexLabel makeVertexLabel(VertexLabelMaker factory) {
        return factory.make();
    }

    /**
     * Whether to ignore undefined types occurring in a query.
     * <p/>
     * If this method returns true, then undefined types referred to in a {@link com.thinkaurelius.titan.core.TitanVertexQuery} will be silently
     * ignored and an empty result set will be returned. If this method returns false, then usage of undefined types
     * in queries results in an {@link IllegalArgumentException}.
     */
    public boolean ignoreUndefinedQueryTypes();



}
