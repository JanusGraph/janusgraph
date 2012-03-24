package com.thinkaurelius.faunus.io.formats.json;

import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusTextInputFormat extends FileInputFormat<LongWritable, FaunusVertex> {

    @Override
    public RecordReader<LongWritable, FaunusVertex> createRecordReader(final InputSplit split, final TaskAttemptContext context) {
        return new FaunusLineRecordReader();
    }

    @Override
    protected boolean isSplitable(final JobContext context, final Path file) {
        return null == new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    }

}