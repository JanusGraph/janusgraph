package com.thinkaurelius.faunus.formats.graphson;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Adopted from Hadoop's FileRecordWriter output.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {
    private static final String UTF8 = "UTF-8";
    private static final byte[] NEWLINE;
    protected DataOutputStream out;

    static {
        try {
            NEWLINE = "\n".getBytes(UTF8);
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("Can not find " + UTF8 + " encoding");
        }
    }


    public GraphSONRecordWriter(final DataOutputStream out) {
        this.out = out;
    }

    @Override
    public void write(final NullWritable key, final FaunusVertex vertex) throws IOException {
        if (null != vertex) {
            this.out.write(GraphSONUtility.toJSON(vertex).toString().getBytes(UTF8));
            this.out.write(NEWLINE);
        }
    }

    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException {
        this.out.close();
    }
}
