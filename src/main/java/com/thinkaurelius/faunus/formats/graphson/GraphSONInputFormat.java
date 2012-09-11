package com.thinkaurelius.faunus.formats.graphson;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Adopted from Hadoop's TextInputFormat source code.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONInputFormat extends FileInputFormat<NullWritable, FaunusVertex> {

    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(final InputSplit split, final TaskAttemptContext context) {
        return new GraphSONRecordReader();
    }

    @Override
    protected boolean isSplitable(final JobContext context, final Path file) {
        return null == new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    }

}