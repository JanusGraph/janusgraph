// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.core.schema;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;

/**
 * When a graph is configured to automatically create vertex/edge labels and property keys when they are first used,
 * a DefaultTypeMaker implementation is used to define them by invoking the {@link #makeVertexLabel(VertexLabelMaker)},
 * {@link #makeEdgeLabel(EdgeLabelMaker)}, or {@link #makePropertyKey(PropertyKeyMaker)} methods respectively.
 * <br>
 * By providing a custom DefaultTypeMaker implementation, one can specify how these types should be defined by default.
 * A DefaultTypeMaker implementation is specified in the graph configuration using the full path which means the
 * implementation must be on the classpath.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see RelationTypeMaker
 */
public interface DefaultSchemaMaker {

    /**
     * Enable/Disable logging in schema maker
     *
     * @param enabled
     */
    default void enableLogging(Boolean enabled) {
        // do nothing
    }

    /**
     * Creates a new edge label with default settings against the provided {@link EdgeLabelMaker}.
     *
     * @param factory EdgeLabelMaker through which the edge label is created
     * @return A new edge label
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    default EdgeLabel makeEdgeLabel(EdgeLabelMaker factory) {
        return factory.directed().make();
    }

    /**
     *
     * @return the default cardinality of a property if created for the given key
     */
    Cardinality defaultPropertyCardinality(String key);

    /**
     * Creates a new property key with default settings against the provided {@link PropertyKeyMaker}.
     *
     * @param factory PropertyKeyMaker through which the property key is created
     * @return A new property key
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    default PropertyKey makePropertyKey(PropertyKeyMaker factory) {
        return factory.cardinality(defaultPropertyCardinality(factory.getName())).dataType(Object.class).make();
    }

    /**
     * Creates a new property key with default settings against the provided {@link PropertyKeyMaker} and value.
     *
     * @param factory PropertyKeyMaker through which the property key is created
     * @param value the value of the property. The default implementation does not use this parameter.
     * @return A new property key
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    default PropertyKey makePropertyKey(PropertyKeyMaker factory, Object value) {
         return makePropertyKey(factory);
    }

    /**
     * Creates a new vertex label with the default settings against the provided {@link VertexLabelMaker}.
     *
     * @param factory VertexLabelMaker through which the vertex label is created
     * @return A new vertex label
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    default VertexLabel makeVertexLabel(VertexLabelMaker factory) {
        return factory.make();
    }

    /**
     * Whether to ignore undefined types occurring in a query.
     * <p>
     * If this method returns true, then undefined types referred to in a {@link org.janusgraph.core.JanusGraphVertexQuery} will be silently
     * ignored and an empty result set will be returned. If this method returns false, then usage of undefined types
     * in queries results in an {@link IllegalArgumentException}.
     */
    boolean ignoreUndefinedQueryTypes();

    /**
     * Add property constraints for a given vertex label using the schema manager.
     *
     * @param vertexLabel to which the constraint applies.
     * @param key defines the property which should be added to the vertex label as a constraint.
     * @param manager is used to update the schema.
     * @see org.janusgraph.core.schema.SchemaManager
     */
    default void makePropertyConstraintForVertex(VertexLabel vertexLabel, PropertyKey key, SchemaManager manager) {
        manager.addProperties(vertexLabel, key);
    }

    /**
     * Add property constraints for a given edge label using the schema manager.
     *
     * @param edgeLabel to which the constraint applies.
     * @param key defines the property which should be added to the edge label as a constraint.
     * @param manager is used to update the schema.
     * @see org.janusgraph.core.schema.SchemaManager
     */
    default void makePropertyConstraintForEdge(EdgeLabel edgeLabel, PropertyKey key, SchemaManager manager) {
        manager.addProperties(edgeLabel, key);
    }

    /**
     * Add a constraint on which vertices the given edge label can connect using the schema manager.
     *
     * @param edgeLabel to which the constraint applies.
     * @param outVLabel specifies the outgoing vertex for this connection.
     * @param inVLabel specifies the incoming vertex for this connection.
     * @param manager is used to update the schema.
     * @see org.janusgraph.core.schema.SchemaManager
     */
    default void makeConnectionConstraint(EdgeLabel edgeLabel, VertexLabel outVLabel, VertexLabel inVLabel, SchemaManager manager) {
        manager.addConnection(edgeLabel, outVLabel, inVLabel);
    }

}
