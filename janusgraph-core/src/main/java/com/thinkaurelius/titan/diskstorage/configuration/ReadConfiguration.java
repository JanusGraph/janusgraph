package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.collect.ImmutableList;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ReadConfiguration {

    public static final ReadConfiguration EMPTY = new ReadConfiguration() {
        @Override
        public<O> O get(String key, Class<O> datatype) {
            return null;
        }

        @Override
        public Iterable<String> getKeys(String prefix) {
            return ImmutableList.of();
        }

        @Override
        public void close() {
            //Nothing
        }
    };

    public<O> O get(String key, Class<O> datatype);

    public Iterable<String> getKeys(String prefix);

    public void close();

}
