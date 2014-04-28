package com.thinkaurelius.faunus.formats.edgelist.rdf;

import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.MapReduceFormat;
import com.thinkaurelius.faunus.formats.edgelist.EdgeListInputMapReduce;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RDFInputFormat extends FileInputFormat<NullWritable, FaunusElement> implements MapReduceFormat {

    public static final String FAUNUS_GRAPH_INPUT_RDF_FORMAT = "faunus.graph.input.rdf.format";
    public static final String FAUNUS_GRAPH_INPUT_RDF_USE_LOCALNAME = "faunus.graph.input.rdf.use-localname";
    public static final String FAUNUS_GRAPH_INPUT_RDF_AS_PROPERTIES = "faunus.graph.input.rdf.as-properties";
    public static final String FAUNUS_GRAPH_INPUT_RDF_LITERAL_AS_PROPERTY = "faunus.graph.input.rdf.literal-as-property";

    public static final String URI = "uri";
    public static final String CONTEXT = "context";
    public static final String NAME = "name";

    @Override
    public RecordReader<NullWritable, FaunusElement> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException {
        return new RDFRecordReader(context.getConfiguration());
    }

    @Override
    protected boolean isSplitable(final JobContext context, final Path file) {
        return null == new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    }

    @Override
    public void addMapReduceJobs(final FaunusCompiler compiler) {
        compiler.addMapReduce(EdgeListInputMapReduce.Map.class,
                EdgeListInputMapReduce.Combiner.class,
                EdgeListInputMapReduce.Reduce.class,
                LongWritable.class,
                FaunusVertex.class,
                NullWritable.class,
                FaunusVertex.class,
                EdgeListInputMapReduce.createConfiguration());
    }
}