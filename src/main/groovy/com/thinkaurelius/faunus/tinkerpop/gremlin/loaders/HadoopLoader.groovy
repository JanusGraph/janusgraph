package com.thinkaurelius.faunus.tinkerpop.gremlin.loaders

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class HadoopLoader {

    public static void load() {

        FileStatus.metaClass.toString = {
            StringBuilder s = new StringBuilder();
            s.append(((FileStatus) delegate).getPermission()).append(" ")
            s.append(((FileStatus) delegate).getOwner()).append(" ");
            s.append(((FileStatus) delegate).getGroup()).append(" ");
            s.append(((FileStatus) delegate).getLen()).append(" ");
            if (((FileStatus) delegate).isDir())
                s.append(((FileStatus) delegate).getPath().getName());
            else
                s.append(((FileStatus) delegate).getPath());
            return s.toString();
        }

        FileSystem.metaClass.ls = { final String path ->
            return ((FileSystem) delegate).listStatus(new Path(path)).collect { it.toString() };
        }

        FileSystem.metaClass.exists = { final String path ->
            return ((FileSystem) delegate).exists(new Path(path));
        }

        FileSystem.metaClass.rm = { final String path ->
            return ((FileSystem) delegate).delete(new Path(path), false);

        }

        FileSystem.metaClass.rmr = { final String path ->
            return ((FileSystem) delegate).delete(new Path(path), true);
        }

        FileSystem.metaClass.copyToLocal = { final String from, final String to ->
            return ((FileSystem) delegate).copyToLocalFile(new Path(from), new Path(to));
        }

        FileSystem.metaClass.copyFromLocal = { final String from, final String to ->
            return ((FileSystem) delegate).copyFromLocalFile(new Path(from), new Path(to));
        }

        FileSystem.metaClass.mergeToLocal = { final String from, final String to ->
            FileSystem fs = (FileSystem) delegate;
            FileSystem local = FileSystem.getLocal(new Configuration());
            FSDataOutputStream outA = local.create(new Path(to));
            FSDataInputStream inA = fs.open(new Path(from));
            int c;
            while ((c = inA.read()) != null) {
                outA.write(c);
            }
        }

        FileSystem.metaClass.more = { final String path, final long totalLines ->
            final FileSystem fs = (FileSystem) delegate;
            final StringBuffer buffer = new StringBuffer();
            long lines = 0;
            HadoopLoader.getAllFilePaths(fs, path, new ArrayList<Path>()).each {
                String line;
                FSDataInputStream reader = fs.open(it);
                while ((line = reader.readLine()) != null && ++lines <= totalLines) {
                    buffer.append(line).append("\n")
                }
            };
            return "\n" + buffer.toString().trim();

        }

        FileSystem.metaClass.more = { final String path ->
            return FileSystem.more(path, Long.MAX_VALUE);
        }
    }

    private static final List<Path> getAllFilePaths(final FileSystem fs, final String path, final List<Path> paths) {
        if (fs.isDirectory(new Path(path))) {
            fs.listStatus(new Path(path)).each { FileStatus status ->
                if (!status.getPath().getName().startsWith("_"))
                    return HadoopLoader.getAllFilePaths(fs, status.getPath().toString(), paths);
            }
        } else {
            paths.add(new Path(path));
        }
        return paths;
    }

}
