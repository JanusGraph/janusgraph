package com.thinkaurelius.faunus.formats.json;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JSONRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {
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


    public JSONRecordWriter(final DataOutputStream out) {
        this.out = out;
    }

    public void write(final NullWritable nullKey, final FaunusVertex vertex) throws IOException {
        if (null != vertex) {
            this.out.write(JSONUtility.toJSON(vertex).toString().getBytes(UTF8));
            this.out.write(NEWLINE);
        }
    }

    public synchronized void close(TaskAttemptContext context) throws IOException {
        out.close();
    }
}
