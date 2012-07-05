package com.thinkaurelius.faunus.io.formats.json;

import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusJSONRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {
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


    public FaunusJSONRecordWriter(final DataOutputStream out) {
        this.out = out;
    }

    public void write(final NullWritable nullKey, final FaunusVertex vertex) throws IOException {
        if (null != vertex) {
            this.out.write(FaunusJSONUtility.toJSON(vertex).toJSONString().getBytes(UTF8));
            this.out.write(NEWLINE);
        }
    }

    public synchronized void close(TaskAttemptContext context) throws IOException {
        out.close();
    }
}
