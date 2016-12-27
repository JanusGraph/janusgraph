package com.thinkaurelius.titan.diskstorage.configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface SystemConfiguration extends Configuration {

    @Override
    public SystemConfiguration restrictTo(final String... umbrellaElements);




}
