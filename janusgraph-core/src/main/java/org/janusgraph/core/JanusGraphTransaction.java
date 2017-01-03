package org.janusgraph.core;

import org.janusgraph.core.schema.SchemaManager;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Collection;
import java.util.Map;

/**
 * JanusGraphTransaction defines a transactional context for a {@link org.janusgraph.core.JanusGraph}. Since JanusGraph is a transactional graph
 * database, all interactions with the graph are mitigated by a JanusGraphTransaction.
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
public interface JanusGraphTransaction extends Graph, SchemaManager {

   /* ---------------------------------------------------------------
    * Modifications
    * ---------------------------------------------------------------
    */

    /**
     * Creates a new vertex in the graph with the vertex label named by the argument.
     *
     * @param vertexLabel the name of the vertex label to use
     * @return a new vertex in the graph created in the context of this transaction
     */
    public JanusVertex addVertex(String vertexLabel);

    @Override
    public JanusVertex addVertex(Object... objects);

    /**
     * @return
     * @see JanusGraph#query()
     */
    public JanusGraphQuery<? extends JanusGraphQuery> query();

    /**
     * Returns a {@link org.janusgraph.core.JanusIndexQuery} to query for vertices or edges against the specified indexing backend using
     * the given query string. The query string is analyzed and answered by the underlying storage backend.
     * <p/>
     * Note, that using indexQuery will may ignore modifications in the current transaction.
     *
     * @param indexName Name of the indexing backend to query as configured
     * @param query Query string
     * @return JanusIndexQuery object to query the index directly
     */
    public JanusIndexQuery indexQuery(String indexName, String query);

    /**
     * @return
     * @see JanusGraph#multiQuery(org.janusgraph.core.JanusVertex...)
     */
    @Deprecated
    public JanusMultiVertexQuery<? extends JanusMultiVertexQuery> multiQuery(JanusVertex... vertices);

    /**
     * @return
     * @see JanusGraph#multiQuery(java.util.Collection)
     */
    @Deprecated
    public JanusMultiVertexQuery<? extends JanusMultiVertexQuery> multiQuery(Collection<JanusVertex> vertices);

    @Override
    public void close();


}
