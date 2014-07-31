package com.thinkaurelius.titan.hadoop.formats.script;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.VertexQueryFilter;
import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.FaunusGremlinScriptEngine;

import org.apache.hadoop.conf.Configuration;
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

import static com.thinkaurelius.titan.hadoop.formats.script.ScriptConfig.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private static final String READ_CALL = "read(vertex,line)";
    private static final String VERTEX = "vertex";
    private static final String LINE = "line";

    private final ScriptEngine engine = new FaunusGremlinScriptEngine();
    private final VertexQueryFilter vertexQuery;
    private final Configuration configuration;
    private final LineRecordReader lineRecordReader;
    private FaunusVertex vertex = new FaunusVertex();

    public ScriptRecordReader(final VertexQueryFilter vertexQuery, final TaskAttemptContext context) throws IOException {
        this.lineRecordReader = new LineRecordReader();
        this.vertexQuery = vertexQuery;
        this.configuration = DEFAULT_COMPAT.getContextConfiguration(context);
        ModifiableHadoopConfiguration faunusConf = ModifiableHadoopConfiguration.of(configuration);
        final FileSystem fs = FileSystem.get(configuration);
        try {
            this.engine.eval(new InputStreamReader(fs.open(new Path(faunusConf.getInputConf(SCRIPT_ROOT).get(SCRIPT_FILE)))));
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        this.lineRecordReader.initialize(genericSplit, context);
    }

    public boolean nextKeyValue() throws IOException {
        while (true) {
            if (!this.lineRecordReader.nextKeyValue())
                return false;
            else {
                try {
                    this.engine.put(LINE, this.lineRecordReader.getCurrentValue().toString());
                    this.vertex = new FaunusVertex(this.configuration);
                    this.engine.put(VERTEX, this.vertex);
                    if ((Boolean) engine.eval(READ_CALL)) {
                        this.vertexQuery.defaultFilter(this.vertex);
                        return true;
                    }
                } catch (Exception e) {
                    throw new IOException(e.getMessage());
                }
            }
        }
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public FaunusVertex getCurrentValue() {
        return this.vertex;
    }

    public float getProgress() throws IOException {
        return this.lineRecordReader.getProgress();
    }

    public synchronized void close() throws IOException {
        this.lineRecordReader.close();
    }
}