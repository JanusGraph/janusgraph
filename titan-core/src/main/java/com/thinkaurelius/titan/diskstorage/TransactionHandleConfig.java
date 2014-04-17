package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public interface TransactionHandleConfig {

    /**
     * Returns the timestamp of this transaction if one has been set.
     *
     * @return custom timestamp for this tx, or null if none is set
     */
    public Long getTimestamp();

    /**
     * Returns the (possibly null) metrics prefix for this transaction.
     *
     * @return metrics name prefix string or null
     */
    public String getMetricsPrefix();

    /**
     * True when {@link #getMetricsPrefix()} is non-null, false when null.
     */
    public boolean hasMetricsPrefix();

    /**
     * Get an arbitrary transaction-specific option.
     *
     * @param opt option for which to return a value
     * @return value of the option
     */
    public <V> V getCustomOption(ConfigOption<V> opt);

    /**
     * Return any transaction-specific options.
     *
     * @see #getCustomOption(ConfigOption)
     * @return options for this tx
     */
    public Configuration getCustomOptions();
}
