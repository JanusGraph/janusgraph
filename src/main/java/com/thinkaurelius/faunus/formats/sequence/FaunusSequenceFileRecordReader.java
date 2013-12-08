package com.thinkaurelius.faunus.formats.sequence;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.SchemaTools;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import org.apache.hadoop.conf.Configuration;
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

    protected SequenceFile.Reader in;
    protected long start;
    protected long end;
    protected boolean more = true;
    protected NullWritable key = null;
    protected FaunusVertex value = null;

    protected boolean pathEnabled = false;

    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        final FileSplit fileSplit = (FileSplit) split;

        final Path path = fileSplit.getPath();
        this.in = new SequenceFile.Reader(path.getFileSystem(context.getConfiguration()), path, context.getConfiguration());
        this.end = fileSplit.getStart() + fileSplit.getLength();

        if (fileSplit.getStart() > in.getPosition()) {
            in.sync(fileSplit.getStart());
        }

        this.start = in.getPosition();
        more = start < end;

        final Configuration schema = SchemaTools.readSchema(context.getConfiguration());
        this.pathEnabled = schema.getBoolean(FaunusCompiler.PATH_ENABLED, false);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!more) {
            return false;
        }
        long pos = in.getPosition();
        key = (NullWritable) in.next((Object) key);
        if (key == null || (pos >= end && in.syncSeen())) {
            more = false;
            key = null;
            value = null;
        } else {
            value = (FaunusVertex) in.getCurrentValue((Object) value);
            value.enablePath(this.pathEnabled);
        }
        return more;
    }

    @Override
    public NullWritable getCurrentKey() {
        return key;
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

}
