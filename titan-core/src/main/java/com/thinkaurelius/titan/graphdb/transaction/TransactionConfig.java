package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Provides functionality to configure a {@link com.thinkaurelius.titan.core.TitanTransaction}.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 * @see com.thinkaurelius.titan.core.TitanTransaction
 */
public class TransactionConfig {

    private final boolean isReadOnly;

    private final boolean assignIDsImmediately;

    private final DefaultTypeMaker defaultTypeMaker;

    private final boolean verifyVertexExistence;

    private final boolean verifyUniqueness;

    private final boolean acquireLocks;

//    private final boolean maintainNewVertices = true;

    private final boolean singleThreaded;

    private final boolean threadBound;

    /**
     * Constructs a new TitanTransaction configuration with default configuration parameters.
     */
    public TransactionConfig(GraphDatabaseConfiguration graphConfig, boolean threadBound) {
        this.isReadOnly = graphConfig.isReadOnly();
        this.assignIDsImmediately = graphConfig.hasFlushIDs();
        this.defaultTypeMaker = graphConfig.getDefaultTypeMaker();
        if (graphConfig.isBatchLoading()) {
            verifyUniqueness = false;
            verifyVertexExistence = false;
            acquireLocks = false;
        } else {
            verifyUniqueness = true;
            verifyVertexExistence = true;
            acquireLocks = true;
        }
        this.threadBound = threadBound;
        singleThreaded = threadBound;
    }

    public TransactionConfig(DefaultTypeMaker defaultTypeMaker, boolean assignIDsImmediately, boolean threadBound) {
        this.defaultTypeMaker = defaultTypeMaker;
        this.assignIDsImmediately = assignIDsImmediately;
        this.isReadOnly = false;
        verifyUniqueness = true;
        verifyVertexExistence = true;
        acquireLocks = true;
        this.threadBound = threadBound;
        singleThreaded = threadBound;
    }

    /**
     * Checks whether the graph transaction is configured as read-only.
     *
     * @return True, if the transaction is configured as read-only, else false.
     */
    public final boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     * @return Whether this transaction is configured to assign idAuthorities immediately.
     */
    public final boolean hasAssignIDsImmediately() {
        return assignIDsImmediately;
    }

    /**
     * Whether the graph transaction is configured to verify that a vertex of a given id actually exists
     * in the database or not.
     *
     * @return True, if vertex existence is verified, else false
     */
    public final boolean hasVerifyVertexExistence() {
        return verifyVertexExistence;
    }

    /**
     * Whether the persistence layer should acquire locks for this transaction during persistence.
     *
     * @return True, if locks should be acquired, else false
     */
    public final boolean hasAcquireLocks() {
        return acquireLocks;
    }

    /**
     * @return The default edge type maker used to automatically create not yet existing edge types.
     */
    public final DefaultTypeMaker getAutoEdgeTypeMaker() {
        return defaultTypeMaker;
    }

    /**
     * Whether the graph transaction is configured to verify that an added key does not yet exist in the database.
     *
     * @return True, if vertex existence is verified, else false
     */
    public final boolean hasVerifyUniqueness() {
        return verifyUniqueness;
    }

    /**
     * Whether this transaction is only accessed by a single thread.
     * If so, then certain data structures may be optimized for single threaded access since locking can be avoided.
     * @return
     */
    public final boolean isSingleThreaded() {
        return singleThreaded;
    }

    /**
     * Whether this transaction is bound to a running thread.
     * If so, then elements in this transaction can expand their life cycle to the next transaction in the thread.
     * @return
     */
    public final boolean isThreadBound() {
        return threadBound;
    }
}
