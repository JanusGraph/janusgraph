package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.core.schema.EdgeLabelMaker;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.schema.VertexLabelMaker;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.ThreadedTransactionalGraph;

import java.util.Collection;

/**
 * Titan graph database implementation of the Blueprint's interface.
 * Use {@link TitanFactory} to open and configure TitanGraph instances.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see TitanFactory
 * @see TitanTransaction
 */
public interface TitanGraph extends Graph, KeyIndexableGraph, ThreadedTransactionalGraph {

   /* ---------------------------------------------------------------
    * Transactions and general admin
    * ---------------------------------------------------------------
    */

    /**
     * Opens a new thread-independent {@link TitanTransaction}.
     * <p/>
     * The transaction is open when it is returned but MUST be explicitly closed by calling {@link com.thinkaurelius.titan.core.TitanTransaction#commit()}
     * or {@link com.thinkaurelius.titan.core.TitanTransaction#rollback()} when it is no longer needed.
     * <p/>
     * Note, that this returns a thread independent transaction object. It is not necessary to call this method
     * to use Blueprint's standard transaction framework which will automatically start a transaction with the first
     * operation on the graph.
     *
     * @return Transaction object representing a transactional context.
     */
    public TitanTransaction newTransaction();

    /**
     * Returns a {@link TransactionBuilder} to construct a new thread-independent {@link TitanTransaction}.
     *
     * @return a new TransactionBuilder
     * @see TransactionBuilder
     * @see #newTransaction()
     */
    public TransactionBuilder buildTransaction();

    /**
     * Returns the management system for this graph instance. The management system provides functionality
     * to change global configuration options, install indexes and inspect the graph schema.
     * <p />
     * The management system operates in its own transactional context which must be explicitly closed.
     *
     * @return
     */
    public TitanManagement getManagementSystem();

    /**
     * Checks whether the graph is still open.
     *
     * @return true, if the graph is open, else false.
     * @see #shutdown()
     */
    public boolean isOpen();

    /**
     * Closes the graph database.
     * <p/>
     * Closing the graph database causes a disconnect and possible closing of the underlying storage backend
     * and a release of all occupied resources by this graph database.
     * Closing a graph database requires that all open thread-independent transactions have been closed -
     * otherwise they will be left abandoned.
     *
     * @throws TitanException if closing the graph database caused errors in the storage backend
     */
    public void shutdown() throws TitanException;

   /* ---------------------------------------------------------------
    * Schema
    * ---------------------------------------------------------------
    */


    /**
     * Returns a {@link com.thinkaurelius.titan.core.schema.PropertyKeyMaker} instance to define a new {@link PropertyKey} with the given name.
     * By defining types explicitly (rather than implicitly through usage) one can control various
     * aspects of the key and associated consistency constraints.
     * <p/>
     *
     * @return a {@link com.thinkaurelius.titan.core.schema.PropertyKeyMaker} instance
     * @see com.thinkaurelius.titan.core.schema.PropertyKeyMaker
     * @see PropertyKey
     */
    public PropertyKeyMaker makePropertyKey(String name);

    /**
     * Returns a {@link com.thinkaurelius.titan.core.schema.EdgeLabelMaker} instance to define a new {@link EdgeLabel} with the given name.
     * By defining types explicitly (rather than implicitly through usage) one can control various
     * aspects of the label and associated consistency constraints.
     * <p/>
     *
     * @return a {@link com.thinkaurelius.titan.core.schema.EdgeLabelMaker} instance
     * @see com.thinkaurelius.titan.core.schema.EdgeLabelMaker
     * @see EdgeLabel
     */
    public EdgeLabelMaker makeEdgeLabel(String name);

    /**
     * Returns the {@link RelationType} uniquely identified by the given name, or NULL if such does not exist.
     *
     * @param name
     * @return
     */
    public RelationType getRelationType(String name);


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
    * Vertices and Querying
    * ---------------------------------------------------------------
    */

    /**
     * Creates a new vertex in the graph with the given vertex label.
     *
     * @return New vertex in the graph created in the context of this transaction.
     */
    public TitanVertex addVertex(VertexLabel vertexLabel);

    /**
     * Returns a {@link TitanGraphQuery} to query for vertices or edges in the graph by their properties.
     *
     * @return
     */
    public TitanGraphQuery<? extends TitanGraphQuery> query();

    /**
     * Returns a {@link TitanIndexQuery} to query for vertices or edges against the specified indexing backend using
     * the given query string. The query string is analyzed and answered by the underlying storage backend.
     * <p/>
     * Note, that using indexQuery may ignore modifications in the current transaction, i.e. the query is not transactionally
     * bound.
     *
     * @param indexName Name of the indexing backend to query as configured
     * @param query Query string
     * @return TitanIndexQuery object to query the index directly
     */
    public TitanIndexQuery indexQuery(String indexName, String query);

    /**
     * Returns a {@link TitanMultiVertexQuery} to query for vertices or edges on multiple vertices simultaneously.
     * This is similar to {@link com.thinkaurelius.titan.core.TitanVertex#query()} but for multiple vertices at once. Hence, this query method is often
     * significantly faster when executing identical {@link TitanVertexQuery} on multiple vertices.
     *
     * @param vertices
     * @return
     */
    public TitanMultiVertexQuery multiQuery(TitanVertex... vertices);

    /**
     * Returns a {@link TitanMultiVertexQuery} to query for vertices or edges on multiple vertices simultaneously.
     * This is similar to {@link com.thinkaurelius.titan.core.TitanVertex#query()} but for multiple vertices at once. Hence, this query method is often
     * significantly faster when executing identical {@link TitanVertexQuery} on multiple vertices.
     *
     * @param vertices
     * @return
     */
    public TitanMultiVertexQuery multiQuery(Collection<TitanVertex> vertices);





}
