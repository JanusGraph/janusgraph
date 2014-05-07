package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Configuration {

    public boolean has(ConfigOption option, String... umbrellaElements);

    public<O> O get(ConfigOption<O> option, String... umbrellaElements);

    public Set<String> getContainedNamespaces(ConfigNamespace umbrella, String... umbrellaElements);

    public Map<String,Object> getSubset(ConfigNamespace umbrella, String... umbrellaElements);

    public Configuration restrictTo(final String... umbrellaElements);


    //--------------------

    public static final Configuration EMPTY = new Configuration() {
        @Override
        public boolean has(ConfigOption option, String... umbrellaElements) {
            return false;
        }

        @Override
        public <O> O get(ConfigOption<O> option, String... umbrellaElements) {
            return option.getDefaultValue();
        }

        @Override
        public Set<String> getContainedNamespaces(ConfigNamespace umbrella, String... umbrellaElements) {
            return Sets.newHashSet();
        }

        @Override
        public Map<String, Object> getSubset(ConfigNamespace umbrella, String... umbrellaElements) {
            return Maps.newHashMap();
        }

        @Override
        public Configuration restrictTo(String... umbrellaElements) {
            return EMPTY;
        }
    };


}
