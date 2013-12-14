package com.thinkaurelius.faunus.mapreduce.util;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyConfiguration extends Configuration {

    private static final EmptyConfiguration immutable = new EmptyConfiguration() {
        @Override
        public void set(String key, String value) {
            throw new IllegalStateException("This empty configuration is immutable");
        }
    };

    public EmptyConfiguration() {
        super(false);
    }

    public static Configuration immutable() {
        return EmptyConfiguration.immutable;
    }
}
