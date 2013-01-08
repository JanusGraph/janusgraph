package com.thinkaurelius.faunus.formats.rexster;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RexsterInputFormat extends InputFormat<NullWritable, FaunusVertex> implements Configurable {

    private long estimatedVertexCount;

    private RexsterConfiguration rexsterConf;

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        final int chunks = jobContext.getConfiguration().getInt("mapred.map.tasks", 1);
        final long chunkSize = (estimatedVertexCount / chunks);

        final List<InputSplit> splits = new ArrayList<InputSplit>();
        for (int i = 0; i < chunks; i++) {
            final RexsterInputSplit split;

            if ((i + 1) == chunks) {
                // the last chunk should run to rexster's end.  since this calculation
                // runs on an estimated count there's no way to know the exact end value.
                split = new RexsterInputSplit(i * chunkSize, Long.MAX_VALUE);
            } else {
                split = new RexsterInputSplit(i * chunkSize, (i * chunkSize) + chunkSize);
            }

            // System.out.println(split);

            splits.add(split);
        }

        return splits;
    }

    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new RexsterRecordReader(this.rexsterConf);
    }

    @Override
    public void setConf(Configuration entries) {
        this.rexsterConf = new RexsterConfiguration(entries);
        this.estimatedVertexCount = this.rexsterConf.getEstimatedVertexCount();
    }

    @Override
    public Configuration getConf() {
        return this.rexsterConf.getConf();
    }
}
