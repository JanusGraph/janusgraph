package com.thinkaurelius.faunus.formats.script;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.tinkerpop.gremlin.FaunusGremlinScriptEngine;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import javax.script.ScriptEngine;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private static final String INPUT_SCRIPT_FILE = "faunus.input.script.file";
    private static final String READ_CALL = "read(vertex,line)";
    private static final String VERTEX = "vertex";
    private static final String LINE = "line";

    private final ScriptEngine engine = new FaunusGremlinScriptEngine();

    private boolean pathEnabled;
    private final LineRecordReader lineRecordReader;
    private FaunusVertex value;

    public ScriptRecordReader(final TaskAttemptContext context) throws IOException {
        this.lineRecordReader = new LineRecordReader();
        this.value = new FaunusVertex();
        this.pathEnabled = context.getConfiguration().getBoolean(FaunusCompiler.PATH_ENABLED, false);

        final FileSystem fs = FileSystem.get(context.getConfiguration());
        try {
            this.engine.eval(new InputStreamReader(fs.open(new Path(context.getConfiguration().get(INPUT_SCRIPT_FILE)))));
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        this.lineRecordReader.initialize(genericSplit, context);
    }

    public boolean nextKeyValue() throws IOException {
        if (!this.lineRecordReader.nextKeyValue())
            return false;

        try {
            this.engine.put(VERTEX, this.value);
            this.engine.put(LINE, this.lineRecordReader.getCurrentValue().toString());
            engine.eval(READ_CALL);
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
        this.value.enablePath(this.pathEnabled);
        return true;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public FaunusVertex getCurrentValue() {
        return this.value;
    }

    public float getProgress() throws IOException {
        return this.lineRecordReader.getProgress();
    }

    public synchronized void close() throws IOException {
        this.lineRecordReader.close();
    }
}