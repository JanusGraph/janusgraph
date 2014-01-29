package com.thinkaurelius.titan.diskstorage;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TransactionHandleConfig {

    public long getTimestamp();

    public boolean hasMetricsPrefix();

    public String getMetricsPrefix();

}
