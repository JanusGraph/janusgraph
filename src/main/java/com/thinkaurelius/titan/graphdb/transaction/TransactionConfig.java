package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.graphdb.blueprints.BlueprintsDefaultTypeMaker;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.lang.builder.HashCodeBuilder;

/***
 * Provides functionality to configure a {@link com.thinkaurelius.titan.core.TitanTransaction}.
 * 
 *
 * @see com.thinkaurelius.titan.core.TitanTransaction
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 *
 */
public class TransactionConfig {

    private boolean isReadOnly = false;
    
    private boolean assignIDsImmediately = false;

    private DefaultTypeMaker defaultTypeMaker = null;
	
	private boolean verifyNodeExistence = true;
	
	private boolean verifyKeyUniqueness = true;

    private boolean acquireLocks = true;
	
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
        }
	}

    public TransactionConfig(DefaultTypeMaker defaultTypeMaker, boolean assignIDsImmediately) {
        this.defaultTypeMaker=defaultTypeMaker;
        this.assignIDsImmediately=assignIDsImmediately;
    }

    public TransactionConfig() {
        this(BlueprintsDefaultTypeMaker.INSTANCE,true);
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
     *
     * @return Whether this transaction is configured to assign ids immediately.
     */
    public boolean assignIDsImmediately() {
        return assignIDsImmediately;
    }

	/**
	 * Whether the graph transaction is configured to verify that a node of a given id actually exists
	 * in the database or not.
	 * 
	 * @return True, if node existence is verified, else false
	 */
	public boolean doVerifyNodeExistence() {
		return verifyNodeExistence;
	}

    /**
     * Whether the persistence layer should acquire locks for this transaction during persistence.
     *
     * @return True, if locks should be acquired, else false
     */
    public boolean acquireLocks() {
        return acquireLocks;
    }

    /**
     *
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
	public boolean doVerifyKeyUniqueness() {
		return verifyKeyUniqueness;
	}

	
}
