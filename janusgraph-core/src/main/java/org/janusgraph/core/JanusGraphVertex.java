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


package org.janusgraph.core;


import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * JanusGraphVertex is the basic unit of a {@link JanusGraph}.
 * It extends the functionality provided by Blueprint's {@link Vertex} by helper and convenience methods.
 * <p>
 * Vertices have incident edges and properties. Edge connect the vertex to other vertices. Properties attach key-value
 * pairs to this vertex to define it.
 * <p>
 * Like {@link JanusGraphRelation} a vertex has a vertex label.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public interface JanusGraphVertex extends JanusGraphElement, Vertex {

    /* ---------------------------------------------------------------
      * Creation and modification methods
      * ---------------------------------------------------------------
      */

    /**
     * Creates a new edge incident on this vertex.
     * <p>
     * Creates and returns a new {@link JanusGraphEdge} of the specified label with this vertex being the outgoing vertex
     * and the given vertex being the incoming vertex.
     * <br>
     * Automatically creates the edge label if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an {@link IllegalArgumentException}.
     *
     * @param label  label of the edge to be created
     * @param vertex incoming vertex of the edge to be created
     * @return new edge
     */
    @Override
    JanusGraphEdge addEdge(String label, Vertex vertex, Object... keyValues);

    /**
     * Creates a new property for this vertex and given key with the specified value.
     * <p>
     * Creates and returns a new {@link JanusGraphVertexProperty} for the given key on this vertex with the specified
     * object being the value.
     * <br>
     * Automatically creates the property key if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an {@link IllegalArgumentException}.
     *
     * @param key       key of the property to be created
     * @param value value of the property to be created
     * @return New property
     * @throws IllegalArgumentException if the value does not match the data type of the property key.
     */
    @Override
    default<V> JanusGraphVertexProperty<V> property(String key, V value) {
        return this.property(key, value, EMPTY_ARGS);
    }

    @Override
    <V> JanusGraphVertexProperty<V> property(final String key, final V value, final Object... keyValues);


    @Override
    <V> JanusGraphVertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues);

     /* ---------------------------------------------------------------
      * Vertex Label
      * ---------------------------------------------------------------
      */

    /**
     * Returns the name of the vertex label for this vertex.
     *
     * @return
     */
    @Override
    default String label() {
        return vertexLabel().name();
    }

    /**
     * Returns the vertex label of this vertex.
     *
     * @return
     */
    VertexLabel vertexLabel();

	/* ---------------------------------------------------------------
     * Incident JanusGraphRelation Access methods
	 * ---------------------------------------------------------------
	 */

    /**
     * Starts a new {@link JanusGraphVertexQuery} for this vertex.
     * <p>
     * Initializes and returns a new {@link JanusGraphVertexQuery} based on this vertex.
     *
     * @return New JanusGraphQuery for this vertex
     * @see JanusGraphVertexQuery
     */
    JanusGraphVertexQuery<? extends JanusGraphVertexQuery> query();

    /**
     * Checks whether this entity has been loaded into the current transaction and modified.
     *
     * @return True, has been loaded and modified, else false.
     */
    boolean isModified();


}
