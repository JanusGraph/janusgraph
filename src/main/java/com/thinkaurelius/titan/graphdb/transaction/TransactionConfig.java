package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.graphdb.edgetypes.StandardDefaultTypeMaker;
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
    
    private boolean assignIDsImmediately = true;

    private boolean autoCreateEdgeTypes = true;
	
	private boolean verifyNodeExistence = true;
	
	private boolean verifyKeyUniqueness = true;
	
	private boolean closed = false;
	
	/**
	 * Constructs a new TitanTransaction configuration with default configuration parameters.
	 */
	public TransactionConfig() {
		
	}
	
	private void verifySetting() {
		if (closed) throw new IllegalStateException("Cannot modify this configuration anymore because it has been closed.");
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
	 * Sets the graph transaction read-only configuration parameter.
	 * 
	 * If readOnly is true, the transaction will be read-only, else the transaction will allow both read
	 * and write/update operations.
	 * 
	 * @param readOnly Whether transaction is configured to be read-only.
	 * @return This TitanTransaction configuration
	 */
	public TransactionConfig setReadOnly(boolean readOnly) {
		verifySetting();
		isReadOnly=readOnly;
		return this;
	}

    /**
     *
     * @return Whether this transaction is configured to assign ids immediately.
     */
    public boolean assignIDsImmediately() {
        return assignIDsImmediately;
    }

    /**
     * Sets the graph transaction to assign ids to all database objects immediately upon creation.
     * If set to true, any object returned by this transaction will have an id (i.e. getID() won't throw an exception).
     * However, the downside is that the transaction cannot assign "efficient" ids, i.e. ids that optimize for storage
     * and retrieval efficiency.
     *
     * @param immediate Whether ids should be assigned immediately upon creation
     * @return This TitanTransaction configuration
     */
    public TransactionConfig setAssignIDsImmediately(boolean immediate) {
        verifySetting();
        this.assignIDsImmediately = immediate;
        return this;
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
	 * Sets the graph transaction to verify node existence or not.
	 * 
	 * @param verify Whether to verify node existence for given node id
	 * @return This Configuration
	 */
	public TransactionConfig setVerifyNodeExistence(boolean verify) {
		verifySetting();
		verifyNodeExistence = verify;
		return this;
	}

    /**
     * Sets the graph transaction to automatically create edge types when a name
     * is provided that does not match any existing edge type.
     *
     * @param createAutomatically Whether to automatically create not yet existing edge types
     * @return This Configuration
     */
    public TransactionConfig setAutoCreateEdgeTypes(boolean createAutomatically) {
        verifySetting();
        autoCreateEdgeTypes=createAutomatically;
        return this;
    }

    /**
     * Whether the graph transaction is configured to automatically create not yet existing edge types.
     * @return True, if edge types are created automatically, else false.
     */
    public boolean doAutoCreateEdgeTypes() {
        return autoCreateEdgeTypes;
    }

    /**
     *
     * @return The default edge type maker used to automatically create not yet existing edge types.
     * @throws UnsupportedOperationException when automatically creating edge types is not supported
     */
    public DefaultTypeMaker getAutoEdgeTypeMaker() {
        if (!doAutoCreateEdgeTypes()) throw new UnsupportedOperationException("Auto edge type creation not supported!");
        else return StandardDefaultTypeMaker.INSTANCE;
    }
	
	/**
	 * Whether the graph transaction is configured to verify that an added key does not yet exist in the database.
	 * 
	 * @return True, if node existence is verified, else false
	 */
	public boolean doVerifyKeyUniqueness() {
		return verifyKeyUniqueness;
	}
	
	/**
	 * Sets the graph transaction to verify key uniqueness or not.
	 * 
	 * @param verify Whether to verify key uniqueness for given node id
	 * @return This Configuration
	 */
	public TransactionConfig setVerifyKeyUniqueness(boolean verify) {
		verifySetting();
		verifyKeyUniqueness = verify;
		return this;
	}
	
	/**
	 * Closes the transaction to fix the current configuration parameters and not allow any further
	 * configuration changes.
	 * 
	 * @return This TitanTransaction configuration
	 */
	public TransactionConfig close() {
		closed = true;
		return this;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder().append(isReadOnly).toHashCode();
	}
	
	@Override
	public boolean equals(Object other) {
		if (this==other) return true;
		else if (!getClass().isInstance(other)) return false;
		TransactionConfig oth = (TransactionConfig)other;
		return this.isReadOnly==oth.isReadOnly;
	}
	
}
