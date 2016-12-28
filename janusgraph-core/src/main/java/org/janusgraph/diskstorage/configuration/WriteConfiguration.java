package org.janusgraph.diskstorage.configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface WriteConfiguration extends ReadConfiguration {

    public<O> void set(String key, O value);

    public void remove(String key);

    public WriteConfiguration copy();

}
