package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.transaction.StandardTransactionBuilder;
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

    /**
     * Opens a new thread-independent {@link TitanTransaction}.
     * <p/>
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
     */
    public TransactionBuilder buildTransaction();


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

    /**
     * Returns a {@link KeyMaker} instance to define a new {@link TitanKey} with the given name.
     * By defining types explicitly (rather than implicitly through usage) one can control various
     * aspects of the key and associated consistency constraints.
     * <p/>
     *
     * @return a {@link KeyMaker} instance
     * @see KeyMaker
     * @see TitanKey
     */
    public KeyMaker makeKey(String name);

    /**
     * Returns a {@link LabelMaker} instance to define a new {@link TitanLabel} with the given name.
     * By defining types explicitly (rather than implicitly through usage) one can control various
     * aspects of the label and associated consistency constraints.
     * <p/>
     *
     * @return a {@link LabelMaker} instance
     * @see LabelMaker
     * @see TitanLabel
     */
    public LabelMaker makeLabel(String name);

    /**
     * Returns an iterable over all defined types that have the given clazz (either {@link TitanLabel} which returns all labels,
     * {@link TitanKey} which returns all keys, or {@link TitanType} which returns all types).
     *
     * @param clazz {@link TitanType} or sub-interface
     * @param <T>
     * @return Iterable over all types for the given category (label, key, or both)
     */
    public <T extends TitanType> Iterable<T> getTypes(Class<T> clazz);

    /**
     * Returns a {@link TitanGraphQuery} to query for vertices or edges in the graph by their properties.
     *
     * @return
     */
    public TitanGraphQuery query();

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


    /**
     * Returns the {@link TitanType} uniquely identified by the given name, or NULL if such does not exist.
     *
     * @param name
     * @return
     */
    public TitanType getType(String name);


    /**
     * Checks whether the graph is still open.
     *
     * @return true, if the graph is open, else false.
     * @see #shutdown()
     */
    public boolean isOpen();


}
