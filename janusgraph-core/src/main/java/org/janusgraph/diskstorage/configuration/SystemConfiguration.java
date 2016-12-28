package org.janusgraph.diskstorage.configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface SystemConfiguration extends Configuration {

    @Override
    public SystemConfiguration restrictTo(final String... umbrellaElements);




}
