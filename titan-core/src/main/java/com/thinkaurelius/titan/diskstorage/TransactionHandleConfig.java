package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public interface TransactionHandleConfig {

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

    public TimestampProvider getTimestampProvider();

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

    public <V> V getCustomOption(ConfigOption<V> opt);

    public Configuration getCustomOptions();
}
