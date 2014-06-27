package com.thinkaurelius.titan.hadoop.mapreduce.util;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyConfiguration extends Configuration {

    private static final EmptyConfiguration immutable = new EmptyConfiguration() {
        @Override
        public void set(final String key, final String value) {
            throw new IllegalStateException("This empty configuration is immutable");
        }
    };

    public EmptyConfiguration() {
        super(true);
        this.clear();
    }

    public static Configuration immutable() {
        return EmptyConfiguration.immutable;
    }
}
