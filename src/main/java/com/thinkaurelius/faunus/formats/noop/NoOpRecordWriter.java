package com.thinkaurelius.faunus.formats.noop;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class NoOpRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {

    public NoOpRecordWriter() {
    }

    public final void write(final NullWritable nullKey, final FaunusVertex vertex) throws IOException {

    }

    public final synchronized void close(final TaskAttemptContext context) throws IOException {
    }
}