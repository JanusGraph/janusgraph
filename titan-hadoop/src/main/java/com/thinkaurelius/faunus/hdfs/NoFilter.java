package com.thinkaurelius.faunus.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class NoFilter implements PathFilter {

    private static final NoFilter INSTANCE = new NoFilter();

    public boolean accept(final Path path) {
        return true;
    }

    public static NoFilter instance() {
        return INSTANCE;
    }
}
