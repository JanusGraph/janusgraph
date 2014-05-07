package com.thinkaurelius.titan.diskstorage.configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ConcurrentWriteConfiguration extends WriteConfiguration {

    public<O> void set(String key, O value, O expectedValue);

}
