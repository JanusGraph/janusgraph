package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.ThreadedTransactionalGraph;

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
     * Returns a {@link TypeMaker} to create a new Titan type.
     *
     * @return
     */
    public TypeMaker makeType();

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
