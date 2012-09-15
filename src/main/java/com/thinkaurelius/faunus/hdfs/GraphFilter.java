package com.thinkaurelius.faunus.hdfs;

import com.thinkaurelius.faunus.Tokens;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphFilter implements PathFilter, Serializable {

    public GraphFilter() {
    }

    public boolean accept(Path path) {
        try {
            if (!path.getFileSystem(new Configuration()).isFile(path))
                return true;
            else
                return path.getName().contains(Tokens.GRAPH) || path.getName().contains(Tokens.PART);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
