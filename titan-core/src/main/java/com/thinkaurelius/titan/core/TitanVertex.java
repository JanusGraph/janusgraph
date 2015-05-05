
package com.thinkaurelius.titan.core;


import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * TitanVertex is the basic unit of a {@link TitanGraph}.
 * It extends the functionality provided by Blueprint's {@link Vertex} by helper and convenience methods.
 * <p />
 * Vertices have incident edges and properties. Edge connect the vertex to other vertices. Properties attach key-value
 * pairs to this vertex to define it.
 * <p />
 * Like {@link TitanRelation} a vertex has a vertex label.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public interface TitanVertex extends TitanElement, Vertex {

    /* ---------------------------------------------------------------
      * Creation and modification methods
      * ---------------------------------------------------------------
      */

    /**
     * Creates a new edge incident on this vertex.
     * <p/>
     * Creates and returns a new {@link TitanEdge} of the specified label with this vertex being the outgoing vertex
     * and the given vertex being the incoming vertex.
     * <br />
     * Automatically creates the edge label if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an {@link IllegalArgumentException}.
     *
     * @param label  label of the edge to be created
     * @param vertex incoming vertex of the edge to be created
     * @return new edge
     */
    @Override
    public TitanEdge addEdge(String label, Vertex vertex, Object... keyValues);

    /**
     * Creates a new property for this vertex and given key with the specified value.
     * <p/>
     * Creates and returns a new {@link TitanVertexProperty} for the given key on this vertex with the specified
     * object being the value.
     * <br />
     * Automatically creates the property key if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an {@link IllegalArgumentException}.
     *
     * @param key       key of the property to be created
     * @param value value of the property to be created
     * @return New property
     * @throws IllegalArgumentException if the value does not match the data type of the property key.
     */
    @Override
    public default<V> TitanVertexProperty<V> property(String key, V value) {
        return this.property(key, value, EMPTY_ARGS);
    }

    @Override
    public <V> TitanVertexProperty<V> property(final String key, final V value, final Object... keyValues);


    @Override
    public <V> TitanVertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues);

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
    public default String label() {
        return vertexLabel().name();
    }

    /**
     * Returns the vertex label of this vertex.
     *
     * @return
     */
    public VertexLabel vertexLabel();

	/* ---------------------------------------------------------------
     * Incident TitanRelation Access methods
	 * ---------------------------------------------------------------
	 */

    /**
     * Starts a new {@link TitanVertexQuery} for this vertex.
     * <p/>
     * Initializes and returns a new {@link TitanVertexQuery} based on this vertex.
     *
     * @return New TitanQuery for this vertex
     * @see TitanVertexQuery
     */
    public TitanVertexQuery<? extends TitanVertexQuery> query();

    /**
     * Checks whether this entity has been loaded into the current transaction and modified.
     *
     * @return True, has been loaded and modified, else false.
     */
    public boolean isModified();


}
