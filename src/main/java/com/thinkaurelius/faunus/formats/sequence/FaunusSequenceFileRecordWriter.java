package com.thinkaurelius.faunus.formats.sequence;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusSequenceFileRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {

    private final SequenceFile.Writer out;

    public FaunusSequenceFileRecordWriter(final SequenceFile.Writer out) {
        this.out = out;
    }

    public void write(final NullWritable key, final FaunusVertex value) throws IOException {
        this.out.append(key, value);
    }

    public void close(final TaskAttemptContext context) throws IOException {
        this.out.close();
    }
}
