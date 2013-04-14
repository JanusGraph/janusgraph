package com.thinkaurelius.faunus.mapreduce.util;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyConfiguration extends Configuration {

    public EmptyConfiguration() {
        super();
        this.clear();
    }
}
