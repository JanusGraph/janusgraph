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

        FileSystem.metaClass.ls = { String path ->
            if (null == path) path = ((FileSystem) delegate).getHomeDirectory().toString();
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
            final FileSystem fs = (FileSystem) delegate;
            final FileSystem local = FileSystem.getLocal(new Configuration());
            final FSDataOutputStream outA = local.create(new Path(to));

            getAllFilePaths(fs, from, []).each {
                final FSDataInputStream inA = fs.open(it);
                int c;
                while ((c = inA.read()) != null) {
                    outA.write(c);
                }
                inA.close();
            }
            outA.close();
        }

        FileSystem.metaClass.head = { final String path, final long totalLines ->
            final FileSystem fs = (FileSystem) delegate;
            List<Path> paths = new LinkedList<Path>(HadoopLoader.getAllFilePaths(fs, path, []));
            if (paths.isEmpty())
                return [];
            else
                return new TextFileIterator(fs, paths, totalLines);

        }


        FileSystem.metaClass.head = {
            final String path ->
            return ((FileSystem) delegate).head(path, Long.MAX_VALUE);
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

    private static class TextFileIterator implements Iterator<String> {
        private final FileSystem fs;
        private final Queue<Path> paths;
        private final long totalLines;
        private long lines = 0;
        private FSDataInputStream reader;
        private String line;


        public TextFileIterator(final FileSystem fs, final Queue<Path> paths, final long totalLines) {
            this.fs = fs;
            this.totalLines = totalLines;
            this.paths = paths;
            this.reader = fs.open(paths.remove());
        }

        public boolean hasNext() {
            if (null != line)
                return true;

            if (this.lines >= this.totalLines || this.reader == null)
                return false;

            this.line = this.reader.readLine();
            if (this.line != null) {
                this.lines++;
                return true;
            } else {
                this.reader.close();
                if (this.paths.isEmpty())
                    this.reader = null;
                else
                    this.reader = this.fs.open(this.paths.remove());
                return this.hasNext();
            }
        }

        public String next() {
            if (null != line) {
                final String temp = line;
                line = null;
                return temp;
            } else if (this.hasNext()) {
                return this.next();
            } else {
                throw new NoSuchElementException();
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }


}
