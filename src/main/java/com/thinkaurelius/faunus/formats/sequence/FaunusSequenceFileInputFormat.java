package com.thinkaurelius.faunus.formats.sequence;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusSequenceFileInputFormat extends SequenceFileInputFilter<NullWritable, FaunusVertex> {

    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new FaunusSequenceFileRecordReader();
    }
}
