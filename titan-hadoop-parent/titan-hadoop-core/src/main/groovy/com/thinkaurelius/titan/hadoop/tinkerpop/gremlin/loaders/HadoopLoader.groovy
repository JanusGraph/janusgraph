package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.loaders

import com.thinkaurelius.titan.hadoop.Tokens
import com.thinkaurelius.titan.hadoop.hdfs.HDFSTools
import com.thinkaurelius.titan.hadoop.hdfs.NoUnderscoreFilter
import com.thinkaurelius.titan.hadoop.hdfs.TextFileLineIterator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
import org.apache.hadoop.io.IOUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class HadoopLoader {

    private static final String SPACE = " ";
    private static final String D_SPACE = "(D) ";

    private static final Logger log =
            LoggerFactory.getLogger(HadoopLoader.class);

    public static void load() {

        FileStatus.metaClass.toString = {
            StringBuilder s = new StringBuilder();
            s.append(((FileStatus) delegate).getPermission()).append(SPACE)
            s.append(((FileStatus) delegate).getOwner()).append(SPACE);
            s.append(((FileStatus) delegate).getGroup()).append(SPACE);
            s.append(((FileStatus) delegate).getLen()).append(SPACE);
            if (((FileStatus) delegate).isDir())
                s.append(D_SPACE);
            s.append(((FileStatus) delegate).getPath().getName());
            return s.toString();
        }

        FileSystem.metaClass.ls = { String path ->
            if (null == path || path.equals("/")) path = ((FileSystem) delegate).getHomeDirectory().toString();
            return ((FileSystem) delegate).globStatus(new Path(path + "/*")).collect { it.toString() };
        }

        FileSystem.metaClass.cp = { final String from, final String to ->
            return FileUtil.copy(((FileSystem) delegate), new Path(from), ((FileSystem) delegate), new Path(to), false, new Configuration());
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

        FileSystem.metaClass.mkdirs = { final String path ->
            return ((FileSystem)delegate).mkdirs(new Path(path));
        }

        FileSystem.metaClass.copyToLocal = { final String from, final String to ->
            return ((FileSystem) delegate).copyToLocalFile(new Path(from), new Path(to));
        }

        FileSystem.metaClass.copyFromLocal = { final String from, final String to ->
            if (from.equals(to) && delegate.equals(FileSystem.getLocal(new Configuration()))) {
                // Copying a local file to itself results in truncation, usually a surprise
                log.info("Not copying local file {} to itself (would result in file truncation)", from);
            } else {
                ((FileSystem) delegate).copyFromLocalFile(new Path(from), new Path(to));
            }
        }

        FileSystem.metaClass.mergeToLocal = { final String from, final String to ->
            final FileSystem fs = (FileSystem) delegate;
            final FileSystem local = FileSystem.getLocal(new Configuration());
            final FSDataOutputStream outA = local.create(new Path(to));

            HDFSTools.getAllFilePaths(fs, new Path(from), NoUnderscoreFilter.instance()).each {
                final FSDataInputStream inA = fs.open(it);
                IOUtils.copyBytes(inA, outA, 8192);
                inA.close();
            }
            outA.close();
        }

        FileSystem.metaClass.head = { final String path, final long totalLines ->
            final FileSystem fs = (FileSystem) delegate;
            final List<Path> paths = new LinkedList<Path>();
            paths.addAll(HDFSTools.getAllFilePaths(fs, new Path(path), NoUnderscoreFilter.instance()));
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
            return HDFSTools.getOutputsFinalJob((FileSystem) delegate, output).toString();
        }
    }


}
