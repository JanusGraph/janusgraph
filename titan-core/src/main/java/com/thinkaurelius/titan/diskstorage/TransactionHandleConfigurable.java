package com.thinkaurelius.titan.diskstorage;


/**
 * An extension to the {@link TransactionHandle} interface that exposes a
 * configuration object of type {@link TransactionHandleConfig} for this particular transaction.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TransactionHandleConfigurable extends TransactionHandle {

    /**
     * Get the configuration for this transaction
     *
     * @return
     */
    public TransactionHandleConfig getConfiguration();

}
