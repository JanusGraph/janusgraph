package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.graphdb.blueprints.BlueprintsDefaultTypeMaker;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.lang.builder.HashCodeBuilder;

/***
 * Provides functionality to configure a {@link com.thinkaurelius.titan.core.TitanTransaction}.
 * 
 * This class give the user fine grained control over the configuration of a {@link com.thinkaurelius.titan.core.TitanTransaction} to be started
 * via the method {@link com.thinkaurelius.titan.core.TitanGraph#startThreadTransaction(TransactionConfig)}.
 * 
 * Note that the configuration parameters cannot be changed once the database has been opened.
 * 
 * @see com.thinkaurelius.titan.core.TitanTransaction
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public class TransactionConfig {

    private boolean isReadOnly = false;
    
    private boolean assignIDsImmediately = false;

    private DefaultTypeMaker defaultTypeMaker = null;
	
	private boolean verifyNodeExistence = true;
	
	private boolean verifyKeyUniqueness = true;
	
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
        }
	}

    public TransactionConfig(DefaultTypeMaker defaultTypeMaker, boolean assignIDsImmediately) {
        this.defaultTypeMaker=defaultTypeMaker;
        this.assignIDsImmediately=assignIDsImmediately;
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
     * Whether the graph transaction is configured to automatically create not yet existing edge types.
     * @return True, if edge types are created automatically, else false.
     */
    public boolean doAutoCreateEdgeTypes() {
        return defaultTypeMaker!=null;
    }

    /**
     *
     * @return The default edge type maker used to automatically create not yet existing edge types.
     * @throws UnsupportedOperationException when automatically creating edge types is not supported
     */
    public DefaultTypeMaker getAutoEdgeTypeMaker() {
        if (!doAutoCreateEdgeTypes()) throw new UnsupportedOperationException("Auto edge type creation not supported!");
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
