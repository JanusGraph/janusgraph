package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.core.schema.EdgeLabelMaker;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.VertexLabelMaker;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;

import java.util.Collection;

/**
 * TitanTransaction defines a transactional context for a {@link TitanGraph}. Since TitanGraph is a transactional graph
 * database, all interactions with the graph are mitigated by a TitanTransaction.
 * <p/>
 * All vertex and edge retrievals are channeled by a graph transaction which bundles all such retrievals, creations and
 * deletions into one transaction. A graph transaction is analogous to a
 * <a href="http://en.wikipedia.org/wiki/Database_transaction">database transaction</a>.
 * The isolation level and <a href="http://en.wikipedia.org/wiki/ACID">ACID support</a> are configured through the storage
 * backend, meaning whatever level of isolation is supported by the storage backend is mirrored by a graph transaction.
 * <p/>
 * A graph transaction supports:
 * <ul>
 * <li>Creating vertices, properties and edges</li>
 * <li>Creating types</li>
 * <li>Index-based retrieval of vertices</li>
 * <li>Querying edges and vertices</li>
 * <li>Aborting and committing transaction</li>
 * </ul>
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public interface TitanTransaction extends TransactionalGraph, KeyIndexableGraph {

   /* ---------------------------------------------------------------
    * Modifications
    * ---------------------------------------------------------------
    */


    /**
     * Creates a new vertex in the graph.
     *
     * @return New vertex in the graph created in the context of this transaction.
     */
    public TitanVertex addVertex();

    /**
     * Creates a new vertex in the graph with the given vertex label name.
     *
     * @return New vertex in the graph created in the context of this transaction.
     */
    public TitanVertex addVertex(String vertexLabel);

    /**
     * Creates a new vertex in the graph with the given vertex label.
     *
     * @return New vertex in the graph created in the context of this transaction.
     */
    public TitanVertex addVertex(VertexLabel vertexLabel);


    /**
     * Creates a new vertex in the graph with the given vertex id and the given vertex label.
     * Note, that an exception is thrown if the vertex id is not a valid Titan vertex id or if a vertex with the given
     * id already exists.
     * <p/>
     * Custom id setting must be enabled via the configuration option {@link com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration#ALLOW_SETTING_VERTEX_ID}.
     * <p/>
     * Use {@link com.thinkaurelius.titan.core.util.TitanId#toVertexId(long)} to construct a valid Titan vertex id from a user id.
     *
     * @param id vertex id of the vertex to be created
     * @param vertexLabel vertex label for this vertex - can be null if no vertex label should be set.
     * @return New vertex
     */
    public TitanVertex addVertex(Long id, VertexLabel vertexLabel);

    /**
     * Creates a new edge connecting the specified vertices.
     * <p/>
     * Creates and returns a new {@link TitanEdge} with given label connecting the vertices in the order
     * specified.
     *
     * @param label     label of the edge to be created
     * @param outVertex outgoing vertex of the edge
     * @param inVertex  incoming vertex of the edge
     * @return new edge
     */
    public TitanEdge addEdge(TitanVertex outVertex, TitanVertex inVertex, EdgeLabel label);

    /**
     * Creates a new edge connecting the specified vertices.
     * <p/>
     * Creates and returns a new {@link TitanEdge} with given label connecting the vertices in the order
     * specified.
     * <br />
     * Automatically creates the edge label if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an {@link IllegalArgumentException}.
     *
     * @param label     label of the edge to be created
     * @param outVertex outgoing vertex of the edge
     * @param inVertex  incoming vertex of the edge
     * @return new edge
     */
    public TitanEdge addEdge(TitanVertex outVertex, TitanVertex inVertex, String label);

    /**
     * Creates a new property for the given vertex and key with the specified value.
     * <p/>
     * Creates and returns a new {@link TitanProperty} with specified property key and the given object being the value.
     *
     * @param key       key of the property to be created
     * @param vertex    vertex for which to create the property
     * @param value value of the property to be created
     * @return new property
     * @throws IllegalArgumentException if the value does not match the data type of the given property key.
     */
    public TitanProperty addProperty(TitanVertex vertex, PropertyKey key, Object value);

    /**
     * Creates a new property for the given vertex and key with the specified value.
     * <p/>
     * Creates and returns a new {@link TitanProperty} with specified property key and the given object being the value.
     * <br />
     * Automatically creates the property key if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an {@link IllegalArgumentException}.
     *
     * @param key       key of the property to be created
     * @param vertex    vertex for which to create the property
     * @param value value of the property to be created
     * @return new property
     * @throws IllegalArgumentException if the value does not match the data type of the given property key.
     */
    public TitanProperty addProperty(TitanVertex vertex, String key, Object value);

    /**
     * Retrieves the vertex for the specified id.
     *
     * @param id id of the vertex to retrieve
     * @return vertex with the given id if it exists, else null
     * @see #containsVertex
     */
    public TitanVertex getVertex(long id);

    /**
     * Checks whether a vertex with the specified id exists in the graph database.
     *
     * @param vertexid vertex id
     * @return true, if a vertex with that id exists, else false
     */
    public boolean containsVertex(long vertexid);

    /**
     * @return
     * @see com.thinkaurelius.titan.core.TitanGraph#query()
     */
    public TitanGraphQuery<? extends TitanGraphQuery> query();

    /**
     * Returns a {@link TitanIndexQuery} to query for vertices or edges against the specified indexing backend using
     * the given query string. The query string is analyzed and answered by the underlying storage backend.
     * <p/>
     * Note, that using indexQuery will may ignore modifications in the current transaction.
     *
     * @param indexName Name of the indexing backend to query as configured
     * @param query Query string
     * @return TitanIndexQuery object to query the index directly
     */
    public TitanIndexQuery indexQuery(String indexName, String query);

    /**
     * @return
     * @see com.thinkaurelius.titan.core.TitanGraph#multiQuery(TitanVertex...)
     */
    public TitanMultiVertexQuery<? extends TitanMultiVertexQuery> multiQuery(TitanVertex... vertices);

    /**
     * @return
     * @see com.thinkaurelius.titan.core.TitanGraph#multiQuery(Collection)
     */
    public TitanMultiVertexQuery<? extends TitanMultiVertexQuery> multiQuery(Collection<TitanVertex> vertices);

    /**
     * Retrieves all vertices which have a property of the given key with the specified value.
     * <p/>
     * For this operation to be efficient, please ensure that the given property key is indexed.
     * Some storage backends may not support this method without a pre-configured index.
     *
     * @param key       key
     * @param value value value
     * @return All vertices which have a property of the given key with the specified value.
     * @see com.thinkaurelius.titan.core.schema.TitanManagement#buildIndex(String, Class)
     */
    public Iterable<TitanVertex> getVertices(PropertyKey key, Object value);

    /**
     * Retrieves all vertices which have a property of the given key with the specified value.
     * <p/>
     * For this operation to be efficient, please ensure that the given property key is indexed.
     * Some storage backends may not support this method without a pre-configured index.
     *
     * @param key       key
     * @param value value value
     * @return All edges which have a property of the given key with the specified value.
     * @see com.thinkaurelius.titan.core.schema.TitanManagement#buildIndex(String, Class)
     */
    public Iterable<TitanEdge> getEdges(PropertyKey key, Object value);

   /* ---------------------------------------------------------------
    * Schema
    * ---------------------------------------------------------------
    */

    /**
     * Checks whether a type with the specified name exists.
     *
     * @param name name of the type
     * @return true, if a type with the given name exists, else false
     */
    public boolean containsRelationType(String name);

    /**
     * Returns the type with the given name.
     * Note, that type names must be unique.
     *
     * @param name name of the type to return
     * @return The type with the given name, or null if such does not exist
     * @see RelationType
     */
    public RelationType getRelationType(String name);

    /**
     * Returns the property key with the given name.
     *
     * @param name name of the property key to return
     * @return the property key with the given name
     * @throws IllegalArgumentException if a property key with the given name does not exist or if the
     *                                  type with the given name is not a property key
     * @see PropertyKey
     */
    public PropertyKey getPropertyKey(String name);

    /**
     * Returns the edge label with the given name.
     *
     * @param name name of the edge label to return
     * @return the edge label with the given name
     * @throws IllegalArgumentException if an edge label with the given name does not exist or if the
     *                                  type with the given name is not an edge label
     * @see EdgeLabel
     */
    public EdgeLabel getEdgeLabel(String name);

    /**
     * Returns a {@link com.thinkaurelius.titan.core.schema.PropertyKeyMaker} instance to define a new {@link PropertyKey} with the given name.
     * By defining types explicitly (rather than implicitly through usage) one can control various
     * aspects of the key and associated consistency constraints.
     * <p/>
     * The key constructed with this maker will be created in the context of this transaction.
     *
     * @return a {@link com.thinkaurelius.titan.core.schema.PropertyKeyMaker} linked to this transaction.
     * @see com.thinkaurelius.titan.core.schema.PropertyKeyMaker
     * @see PropertyKey
     */
    public PropertyKeyMaker makePropertyKey(String name);

    /**
     * Returns a {@link com.thinkaurelius.titan.core.schema.EdgeLabelMaker} instance to define a new {@link EdgeLabel} with the given name.
     * By defining types explicitly (rather than implicitly through usage) one can control various
     * aspects of the label and associated consistency constraints.
     * <p/>
     * The label constructed with this maker will be created in the context of this transaction.
     *
     * @return a {@link com.thinkaurelius.titan.core.schema.EdgeLabelMaker} linked to this transaction.
     * @see com.thinkaurelius.titan.core.schema.EdgeLabelMaker
     * @see EdgeLabel
     */
    public EdgeLabelMaker makeEdgeLabel(String name);

    /**
     * Whether a vertex label with the given name exists in the graph.
     *
     * @param name
     * @return
     */
    public boolean containsVertexLabel(String name);

    /**
     * Returns the vertex label with the given name. If a vertex label with this name does not exist, the label is
     * automatically created through the registered {@link com.thinkaurelius.titan.core.schema.DefaultSchemaMaker}.
     * <p />
     * Attempting to automatically create a vertex label might cause an exception depending on the configuration.
     *
     * @param name
     * @return
     */
    public VertexLabel getVertexLabel(String name);

    /**
     * Returns a {@link VertexLabelMaker} to define a new vertex label with the given name. Note, that the name must
     * be unique.
     *
     * @param name
     * @return
     */
    public VertexLabelMaker makeVertexLabel(String name);

   /* ---------------------------------------------------------------
    * Closing and admin
    * ---------------------------------------------------------------
    */

    /**
     * Commits and closes the transaction.
     * <p/>
     * Will attempt to persist all modifications which may result in exceptions in case of persistence failures or
     * lock contention.
     * <br />
     * The call releases data structures if possible. All element references (e.g. vertex objects) retrieved
     * through this transaction are stale after the transaction closes and should no longer be used.
     *
     * @throws com.thinkaurelius.titan.diskstorage.StorageException
     *          if an error arises during persistence
     */
    public void commit();

    /**
     * Aborts and closes the transaction. Will discard all modifications.
     * <p/>
     * The call releases data structures if possible. All element references (e.g. vertex objects) retrieved
     * through this transaction are stale after the transaction closes and should no longer be used.
     *
     * @throws com.thinkaurelius.titan.diskstorage.StorageException
     *          if an error arises when releasing the transaction handle
     */
    public void rollback();

    /**
     * Checks whether the transaction is still open.
     *
     * @return true, when the transaction is open, else false
     */
    public boolean isOpen();

    /**
     * Checks whether the transaction has been closed.
     *
     * @return true, if the transaction has been closed, else false
     */
    public boolean isClosed();

    /**
     * Checks whether any changes to the graph database have been made in this transaction.
     * <p/>
     * A modification may be an edge or vertex update, addition, or deletion.
     *
     * @return true, if the transaction contains updates, else false.
     */
    public boolean hasModifications();

}
