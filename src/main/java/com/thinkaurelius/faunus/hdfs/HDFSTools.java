package com.thinkaurelius.faunus.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HDFSTools {

    public static List<Path> getAllFilePaths(final FileSystem fs, final String path, final List<Path> paths) throws IOException {
        Path p = new Path(path);
        if (fs.isDirectory(p))
            p = new Path(path + "/*");

        final FileStatus[] statuses = fs.globStatus(p);
        if (null == statuses)
            throw new IOException("No such path: " + p);

        for (FileStatus status : statuses) {
            if (!status.getPath().getName().startsWith("_")) {
                if (fs.isDirectory(status.getPath()))
                    return getAllFilePaths(fs, status.getPath().getName(), paths);
                else
                    paths.add(status.getPath());
            }
        }
        return paths;
    }
}
