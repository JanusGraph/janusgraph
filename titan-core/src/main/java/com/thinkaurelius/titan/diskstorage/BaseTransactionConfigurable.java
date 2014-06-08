package com.thinkaurelius.titan.diskstorage;


/**
 * An extension to the {@link BaseTransaction} interface that exposes a
 * configuration object of type {@link BaseTransactionConfig} for this particular transaction.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface BaseTransactionConfigurable extends BaseTransaction {

    /**
     * Get the configuration for this transaction
     *
     * @return
     */
    public BaseTransactionConfig getConfiguration();

}
