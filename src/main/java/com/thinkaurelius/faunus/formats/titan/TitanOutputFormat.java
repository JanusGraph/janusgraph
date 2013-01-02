package com.thinkaurelius.faunus.formats.titan;

import java.io.IOException;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

public abstract class TitanOutputFormat extends OutputFormat<NullWritable, FaunusVertex> implements Configurable {

    protected StandardTitanGraph graph;
    protected Configuration config;

    @Override
    public void checkOutputSpecs(final JobContext context) throws InterruptedException, IOException {
    }

    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws InterruptedException, IOException {
        return new ColumnFamilyOutputFormat.NullOutputCommitter();
    }

    @Override
    public RecordWriter<NullWritable, FaunusVertex> getRecordWriter(final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TitanRecordWriter(this.graph);
    }

    @Override
    public Configuration getConf() {
        return this.config;
    }
}