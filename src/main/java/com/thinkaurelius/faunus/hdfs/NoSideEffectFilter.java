package com.thinkaurelius.faunus.hdfs;

import com.thinkaurelius.faunus.Tokens;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class NoSideEffectFilter implements PathFilter {

    public NoSideEffectFilter() {
    }

    public boolean accept(Path path) {
        try {
            if (!path.getFileSystem(new Configuration()).isFile(path))
                return true;
            else
                return !path.getName().startsWith(Tokens.SIDEEFFECT);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
