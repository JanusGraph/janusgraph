package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.graphdb.blueprints.BlueprintsDefaultTypeMaker;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Provides functionality to configure a {@link com.thinkaurelius.titan.core.TitanTransaction}.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 * @see com.thinkaurelius.titan.core.TitanTransaction
 */
public class TransactionConfig {

    private boolean isReadOnly = false;

    private boolean assignIDsImmediately = false;

    private DefaultTypeMaker defaultTypeMaker = null;

    private boolean verifyNodeExistence = true;

    private boolean verifyKeyUniqueness = true;

    private boolean acquireLocks = true;

    private boolean maintainNewVertices = true;

    /**
     * Constructs a new TitanTransaction configuration with default configuration parameters.
     */
    public TransactionConfig(GraphDatabaseConfiguration graphConfig) {
        this.isReadOnly = graphConfig.isReadOnly();
        this.assignIDsImmediately = graphConfig.hasFlushIDs();
        this.defaultTypeMaker = graphConfig.getDefaultTypeMaker();
        if (graphConfig.isBatchLoading()) {
            verifyKeyUniqueness = false;
            verifyNodeExistence = false;
            acquireLocks = false;
            maintainNewVertices = false;
        }
    }

    public TransactionConfig(DefaultTypeMaker defaultTypeMaker, boolean assignIDsImmediately) {
        this.defaultTypeMaker = defaultTypeMaker;
        this.assignIDsImmediately = assignIDsImmediately;
    }

    public TransactionConfig() {
        this(BlueprintsDefaultTypeMaker.INSTANCE, true);
    }

    /**
     * Checks whether the graph transaction is configured as read-only.
     *
     * @return True, if the transaction is configured as read-only, else false.
     */
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     * @return Whether this transaction is configured to assign idAuthorities immediately.
     */
    public boolean hasAssignIDsImmediately() {
        return assignIDsImmediately;
    }

    /**
     * Whether the graph transaction is configured to verify that a node of a given id actually exists
     * in the database or not.
     *
     * @return True, if node existence is verified, else false
     */
    public boolean hasVerifyNodeExistence() {
        return verifyNodeExistence;
    }

    /**
     * Whether the persistence layer should acquire locks for this transaction during persistence.
     *
     * @return True, if locks should be acquired, else false
     */
    public boolean hasAcquireLocks() {
        return acquireLocks;
    }

    /**
     * @return The default edge type maker used to automatically create not yet existing edge types.
     */
    public DefaultTypeMaker getAutoEdgeTypeMaker() {
        return defaultTypeMaker;
    }

    /**
     * Whether the graph transaction is configured to verify that an added key does not yet exist in the database.
     *
     * @return True, if node existence is verified, else false
     */
    public boolean hasVerifyKeyUniqueness() {
        return verifyKeyUniqueness;
    }

    /**
     * Whether the graph transaction is configured to maintain a set of all newly created vertices in that
     * transaction. Disabling the maintenance of new vertices saves memory at the expense of not being able
     * to iterate over all vertices in the transaction (in case vertex iteration is supported).
     * Hence, disabling it only makes sense in batch loading scenarios.
     *
     * @return True, if new vertices are maintained, else false
     */
    public boolean hasMaintainNewVertices() {
        return maintainNewVertices;
    }


}
