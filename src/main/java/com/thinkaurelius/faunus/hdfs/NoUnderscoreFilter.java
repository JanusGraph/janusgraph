package com.thinkaurelius.faunus.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class NoUnderscoreFilter implements PathFilter {

    private static final NoUnderscoreFilter INSTANCE = new NoUnderscoreFilter();

    public boolean accept(final Path path) {
        return !path.getName().startsWith("_");
    }

    public static NoUnderscoreFilter instance() {
        return INSTANCE;
    }
}