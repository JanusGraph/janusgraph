package com.thinkaurelius.faunus.formats.script;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.VertexQueryFilter;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * ScriptInputFormat supports the arbitrary parsing of a \n-based file format.
 * Each line of the file is passed to the Gremlin/Groovy script identified by the faunus.input.script.file property.
 * The Gremlin/Groovy file must have a method with the following signature:
 * <p/>
 * def boolean read(FaunusVertex vertex, String line) { ... }
 * <p/>
 * The FaunusVertex argument is a reusable object to avoid object creation (see FaunusVertex.reuse(long)).
 * The String argument is the \n-line out of the file at the faunus.input.location.
 * The boolean denotes whether or not the provided line yielded a successful creation of a FaunusVertex.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptInputFormat extends FileInputFormat<NullWritable, FaunusVertex> implements Configurable {

    public static final String FAUNUS_GRAPH_INPUT_SCRIPT_FILE = "faunus.graph.input.script.file";
    private VertexQueryFilter vertexQuery;
    private Configuration config;

    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException {
        return new ScriptRecordReader(this.vertexQuery, context);
    }

    @Override
    protected boolean isSplitable(final JobContext context, final Path file) {
        return null == new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    }

    @Override
    public void setConf(final Configuration config) {
        this.config = config;
        this.vertexQuery = VertexQueryFilter.create(config);
    }

    @Override
    public Configuration getConf() {
        return this.config;
    }

}