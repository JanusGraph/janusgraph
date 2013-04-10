package com.thinkaurelius.faunus.formats.script;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.tinkerpop.gremlin.FaunusGremlinScriptEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {
    protected final DataOutputStream out;
    private final ScriptEngine engine = new FaunusGremlinScriptEngine();

    private static final String WRITE_CALL = "write(vertex)";
    private static final String VERTEX = "vertex";
    // TODO: make it work with the DataOutputStream passed into the write() method
    // TODO: if you can't do this, then make a null return be a skip
    // private static final String OUT = "out";

    private static final String UTF8 = "UTF-8";
    private static final byte[] NEWLINE;

    static {
        try {
            NEWLINE = "\n".getBytes(UTF8);
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("Can not find " + UTF8 + " encoding");
        }
    }

    public ScriptRecordWriter(final DataOutputStream out, final Configuration configuration) throws IOException {
        this.out = out;
        final FileSystem fs = FileSystem.get(configuration);
        try {
            // this.engine.put(OUT, this.out);
            this.engine.eval(new InputStreamReader(fs.open(new Path(configuration.get(ScriptOutputFormat.OUTPUT_SCRIPT_FILE)))));
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    public void write(final NullWritable key, final FaunusVertex vertex) throws IOException {
        if (null != vertex) {
            try {
                this.engine.put(VERTEX, vertex);
                this.out.write(((String) this.engine.eval(WRITE_CALL)).getBytes(UTF8));
                this.out.write(NEWLINE);
            } catch (final ScriptException e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    public synchronized void close(TaskAttemptContext context) throws IOException {
        this.out.close();
    }
}
