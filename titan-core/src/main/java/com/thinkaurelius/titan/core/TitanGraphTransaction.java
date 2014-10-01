package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.core.schema.SchemaManager;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;

import java.util.Collection;
import java.util.Map;

/**
 * TitanGraphTransaction defines a transactional context for a {@link com.thinkaurelius.titan.core.TitanGraph}. Since TitanGraph is a transactional graph
 * database, all interactions with the graph are mitigated by a TitanGraphTransaction.
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
public interface TitanGraphTransaction extends TransactionalGraph, KeyIndexableGraph, SchemaManager {

   /* ---------------------------------------------------------------
    * Modifications
    * ---------------------------------------------------------------
    */

    /**
     * Creates a new vertex in the graph with the default vertex label.
     *
     * @return New vertex in the graph created in the context of this transaction.
     */
    public TitanVertex addVertex();

    /**
     * Creates a new vertex in the graph with the vertex label named by the argument.
     *
     * @param vertexLabel the name of the vertex label to use
     * @return a new vertex in the graph created in the context of this transaction
     */
    public TitanVertex addVertexWithLabel(String vertexLabel);

    /**
     * Creates a new vertex in the graph with the given vertex label.
     *
     * @param vertexLabel the vertex label which will apply to the new vertex
     * @return a new vertex in the graph created in the context of this transaction
     */
    public TitanVertex addVertexWithLabel(VertexLabel vertexLabel);

    /**
     * Retrieves the vertex for the specified id.
     *
     * @param id id of the vertex to retrieve
     * @return vertex with the given id if it exists, else null
     * @see #containsVertex
     */
    public TitanVertex getVertex(long id);

    /**
     * Retrieves the vertices for the given ids and returns a map from those ids to the corresponding vertices.
     * If a given id does not identify a vertex, it is not included in the returned map
     *
     * @param ids array of ids for which to retrieve vertices
     * @return map from ids to corresponding vertices
     * does not identify a vertex
     */
    public Map<Long,TitanVertex> getVertices(long... ids);

    /**
     * Checks whether a vertex with the specified id exists in the graph database.
     *
     * @param vertexid vertex id
     * @return true, if a vertex with that id exists, else false
     */
    public boolean containsVertex(long vertexid);

    /**
     * @return
     * @see TitanGraph#query()
     */
    public TitanGraphQuery<? extends TitanGraphQuery> query();

    /**
     * Returns a {@link com.thinkaurelius.titan.core.TitanIndexQuery} to query for vertices or edges against the specified indexing backend using
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
     * @see TitanGraph#multiQuery(com.thinkaurelius.titan.core.TitanVertex...)
     */
    public TitanMultiVertexQuery<? extends TitanMultiVertexQuery> multiQuery(TitanVertex... vertices);

    /**
     * @return
     * @see TitanGraph#multiQuery(java.util.Collection)
     */
    public TitanMultiVertexQuery<? extends TitanMultiVertexQuery> multiQuery(Collection<TitanVertex> vertices);


}
