package com.thinkaurelius.titan.hadoop.formats.edgelist.rdf;

import com.thinkaurelius.titan.hadoop.FaunusElement;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;
import com.thinkaurelius.titan.hadoop.formats.MapReduceFormat;
import com.thinkaurelius.titan.hadoop.formats.edgelist.EdgeListInputMapReduce;

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

    public static final String TITAN_HADOOP_GRAPH_INPUT_RDF_FORMAT = "titan.hadoop.input.rdf.format";
    public static final String TITAN_HADOOP_GRAPH_INPUT_RDF_USE_LOCALNAME = "titan.hadoop.input.rdf.use-localname";
    public static final String TITAN_HADOOP_GRAPH_INPUT_RDF_AS_PROPERTIES = "titan.hadoop.input.rdf.as-properties";
    public static final String TITAN_HADOOP_GRAPH_INPUT_RDF_LITERAL_AS_PROPERTY = "titan.hadoop.input.rdf.literal-as-property";

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
    public void addMapReduceJobs(final HadoopCompiler compiler) {
        compiler.addMapReduce(EdgeListInputMapReduce.Map.class,
                EdgeListInputMapReduce.Combiner.class,
                EdgeListInputMapReduce.Reduce.class,
                LongWritable.class,
                HadoopVertex.class,
                NullWritable.class,
                HadoopVertex.class,
                EdgeListInputMapReduce.createConfiguration());
    }
}