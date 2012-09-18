package com.thinkaurelius.faunus.tinkerpop.gremlin.loaders

import com.thinkaurelius.faunus.Tokens
import com.thinkaurelius.faunus.hdfs.HDFSTools
import com.thinkaurelius.faunus.hdfs.TextFileLineIterator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils

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
                s.append("(D) ");
            s.append(((FileStatus) delegate).getPath().getName());
            return s.toString();
        }

        FileSystem.metaClass.ls = { String path ->
            if (null == path || path.equals("/")) path = ((FileSystem) delegate).getHomeDirectory().toString();
            return ((FileSystem) delegate).globStatus(new Path(path + "/*")).collect { it.toString() };
        }

        FileSystem.metaClass.exists = { final String path ->
            return ((FileSystem) delegate).exists(new Path(path));
        }

        FileSystem.metaClass.rm = { final String path ->
            HDFSTools.globDelete((FileSystem) delegate, path, false);

        }

        FileSystem.metaClass.rmr = { final String path ->
            HDFSTools.globDelete((FileSystem) delegate, path, true);
        }

        FileSystem.metaClass.copyToLocal = { final String from, final String to ->
            return ((FileSystem) delegate).copyToLocalFile(new Path(from), new Path(to));
        }

        FileSystem.metaClass.copyFromLocal = { final String from, final String to ->
            return ((FileSystem) delegate).copyFromLocalFile(new Path(from), new Path(to));
        }

        FileSystem.metaClass.mergeToLocal = { final String from, final String to ->
            final FileSystem fs = (FileSystem) delegate;
            final FileSystem local = FileSystem.getLocal(new Configuration());
            final FSDataOutputStream outA = local.create(new Path(to));

            HDFSTools.getAllFilePaths(fs, from).each {
                final FSDataInputStream inA = fs.open(it);
                IOUtils.copyBytes(inA, outA, 8192);
                inA.close();
            }
            outA.close();
        }

        FileSystem.metaClass.head = { final String path, final long totalLines ->
            final FileSystem fs = (FileSystem) delegate;
            List<Path> paths = new LinkedList<Path>();
            paths.addAll(HDFSTools.getAllFilePaths(fs, path));
            if (paths.isEmpty())
                return Collections.emptyList();
            else
                return new TextFileLineIterator(fs, paths, totalLines);

        }


        FileSystem.metaClass.head = {
            final String path ->
            return ((FileSystem) delegate).head(path, Long.MAX_VALUE);
        }

        FileSystem.metaClass.unzip = { final String from, final String to, final boolean deleteZip ->
            HDFSTools.decompressPath((FileSystem) delegate, from, to, Tokens.BZ2, deleteZip);
        }

        FileSystem.metaClass.result = { final String output ->
            return HDFSTools.getOutputsFinalJob((FileSystem) delegate, output);

        }
    }


}
