package com.thinkaurelius.faunus.formats.sequence.faunus01;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;

/**
 * Reads a [NullWritable,FaunusVertex] SequenceFile as encoded in Faunus 0.1.z.
 * Writes a [NullWrtiable,FaunusVertex] SequenceFile as encoded in the current Faunus version.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusSequenceFileInputFormat extends SequenceFileInputFormat<NullWritable, FaunusVertex> {

    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException {
        return new FaunusSequenceFileRecordReader();
    }

}
