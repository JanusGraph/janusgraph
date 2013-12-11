package com.thinkaurelius.titan.diskstorage.configuration;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Configuration {

    public<O> O get(ConfigOption<O> option, String... umbrellaElements);

    public Set<String> getContainedNamespaces(ConfigNamespace umbrella, String... umbrellaElements);


}
