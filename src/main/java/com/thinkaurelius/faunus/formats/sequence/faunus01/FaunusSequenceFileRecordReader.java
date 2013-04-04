package com.thinkaurelius.faunus.formats.sequence.faunus01;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusSequenceFileRecordReader extends RecordReader<NullWritable, FaunusVertex> {
    private SequenceFile.Reader in;
    private long start;
    private long end;
    private boolean more = true;
    private NullWritable key = null;
    private FaunusVertex value = null;
    protected Configuration conf;

    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        final FileSplit fileSplit = (FileSplit) split;
        this.conf = context.getConfiguration();
        final Path path = fileSplit.getPath();
        final FileSystem fs = path.getFileSystem(conf);
        this.in = new SequenceFile.Reader(fs, path, conf);
        this.end = fileSplit.getStart() + fileSplit.getLength();

        if (fileSplit.getStart() > in.getPosition()) {
            in.sync(fileSplit.getStart());                  // sync to start
        }

        this.start = in.getPosition();
        more = start < end;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!more) {
            return false;
        }
        long pos = in.getPosition();
        in.next(key);
        if (key == null || (pos >= end && in.syncSeen())) {
            more = false;
            key = null;
            value = null;
        } else {
            value = buildFaunusVertex(in);
        }
        return more;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public FaunusVertex getCurrentValue() {
        return value;
    }

    public float getProgress() throws IOException {
        if (end == start) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (in.getPosition() - start) / (float) (end - start));
        }
    }

    public synchronized void close() throws IOException {
        in.close();
    }

    private FaunusVertex buildFaunusVertex(SequenceFile.Reader in) {
        return null;
        //in.createValueBytes().writeUncompressedBytes();
        //FaunusVertex vertex = new FaunusVertex(WritableUtils.readVLong());
    }

}
