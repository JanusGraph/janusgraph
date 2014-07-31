package com.thinkaurelius.titan.hadoop.formats.edgelist.rdf;

import com.thinkaurelius.titan.hadoop.FaunusElement;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
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

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RDFInputFormat extends FileInputFormat<NullWritable, FaunusElement> implements MapReduceFormat {

    @Override
    public RecordReader<NullWritable, FaunusElement> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException {
        return new RDFRecordReader(DEFAULT_COMPAT.getContextConfiguration(context));
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
                FaunusVertex.class,
                NullWritable.class,
                FaunusVertex.class,
                EdgeListInputMapReduce.createConfiguration());
    }
}