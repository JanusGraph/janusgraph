package com.thinkaurelius.titan.hadoop.hdfs;

import com.thinkaurelius.titan.hadoop.Tokens;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TextFileLineIterator implements Iterator<String> {
    private final FileSystem fs;
    private final Queue<Path> paths;
    private final long totalLines;
    private long lines = 0;
    private BufferedReader reader = null;
    private String line;
    private CompressionCodec codec = new BZip2Codec();


    public TextFileLineIterator(final FileSystem fs, final Queue<Path> paths, final long totalLines) throws IOException {
        this.fs = fs;
        this.totalLines = totalLines;
        this.paths = paths;
    }

    public TextFileLineIterator(final FileSystem fs, final FileStatus[] statuses, final long totalLines) throws IOException {
        this.fs = fs;
        this.totalLines = totalLines;
        this.paths = new LinkedList<Path>();
        for (final FileStatus status : statuses) {
            this.paths.add(status.getPath());
        }
    }

    public boolean hasNext() {
        if (null != line)
            return true;

        if (this.lines >= this.totalLines)
            return false;

        try {
            if (this.reader == null)
                if (this.paths.isEmpty())
                    return false;
                else
                    this.reader = this.getUncompressedInputStream();

            this.line = this.reader.readLine();
            if (this.line != null) {
                this.lines++;
                return true;
            } else {
                this.reader.close();
                if (this.paths.isEmpty())
                    this.reader = null;
                else
                    this.reader = this.getUncompressedInputStream();
                return this.hasNext();
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
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

    private BufferedReader getUncompressedInputStream() throws IOException {
        final Path path = this.paths.remove();
        if (path.getName().endsWith(Tokens.BZ2))
            return new BufferedReader(new InputStreamReader(this.codec.createInputStream(fs.open(path))));
        else
            return new BufferedReader(new InputStreamReader(this.fs.open(path)));
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}
