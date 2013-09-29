package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Provides configuration options for {@link com.thinkaurelius.titan.core.TitanTransaction}.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 * @see com.thinkaurelius.titan.core.TitanTransaction
 */
public interface TransactionConfiguration {

    /**
     * Checks whether the graph transaction is configured as read-only.
     *
     * @return True, if the transaction is configured as read-only, else false.
     */
    public boolean isReadOnly();

    /**
     * @return Whether this transaction is configured to assign idAuthorities immediately.
     */
    public boolean hasAssignIDsImmediately();

    /**
     * Whether the graph transaction is configured to verify that a vertex with the id GIVEN BY THE USER actually exists
     * in the database or not.
     * In other words, it is verified that user provided vertex ids (through public APIs) actually exist.
     *
     * @return True, if vertex existence is verified, else false
     */
    public boolean hasVerifyExternalVertexExistence();

    /**
     * Whether the graph transaction is configured to verify that a vertex with the id actually exists
     * in the database or not on every retrieval.
     * In other words, it is always verified that a vertex for a given id exists, even if that id is retrieved internally
     * (through private APIs).
     * <p/>
     * Hence, this is a defensive setting against data degradation, where edges and/or index entries might point to no
     * longer existing vertices. Use this setting with caution as it introduces additional overhead entailed by checking
     * the existence.
     * <p/>
     * Unlike {@link #hasVerifyExternalVertexExistence()} this is about internally verifying ids.
     *
     * @return True, if vertex existence is verified, else false
     */
    public boolean hasVerifyInternalVertexExistence();

    /**
     * Whether the persistence layer should acquire locks for this transaction during persistence.
     *
     * @return True, if locks should be acquired, else false
     */
    public boolean hasAcquireLocks();

    /**
     * @return The default edge type maker used to automatically create not yet existing edge types.
     */
    public DefaultTypeMaker getAutoEdgeTypeMaker();

    /**
     * Whether the graph transaction is configured to verify that an added key does not yet exist in the database.
     *
     * @return True, if vertex existence is verified, else false
     */
    public boolean hasVerifyUniqueness();

    /**
     * Whether this transaction is only accessed by a single thread.
     * If so, then certain data structures may be optimized for single threaded access since locking can be avoided.
     *
     * @return
     */
    public boolean isSingleThreaded();

    /**
     * Whether this transaction is bound to a running thread.
     * If so, then elements in this transaction can expand their life cycle to the next transaction in the thread.
     *
     * @return
     */
    public boolean isThreadBound();

    /**
     * The size of the vertex cache for this particular transaction, i.e. the maximum number
     * of non-modified vertices that are kept in cache
     *
     * @return
     */
    public long getVertexCacheSize();

    /**
     * The maximum weight for the index cache store used in this particular transaction
     *
     * @return
     */
    public long getIndexCacheWeight();


    /**
     * Whether a timestamp has been configured for this transaction
     *
     * @return
     */
    public boolean hasTimestamp();

    /**
     * Returns the timestamp of this transaction if one has been set, otherwise throws an exception
     *
     * @return
     * @see #hasTimestamp()
     */
    public long getTimestamp();


}
