package com.thinkaurelius.faunus.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SuffixFilter implements PathFilter {

    private final String suffix;

    public SuffixFilter(final String suffix) {
        this.suffix = suffix;
    }

    public boolean accept(final Path path) {
        try {
            return path.getName().endsWith(this.suffix);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
