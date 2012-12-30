package com.thinkaurelius.faunus.formats.titan.cassandra;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.titan.hbase.FaunusTitanHBaseGraph;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {

    private final FaunusTitanCassandraGraph graph;
    private final RecordWriter<NullWritable, FaunusVertex> writer;

    public TitanCassandraRecordWriter(final FaunusTitanCassandraGraph graph, RecordWriter<NullWritable, FaunusVertex> writer) {
        this.graph = graph;
        this.writer = writer;
    }

    public void close(final TaskAttemptContext context) throws InterruptedException, IOException {
        this.writer.close(context);
    }

    public void write(final NullWritable key, final FaunusVertex value) throws InterruptedException, IOException {
        //this.writer.write(key, value);
        //TODO: Must be a PUT or DELETE
    }
}