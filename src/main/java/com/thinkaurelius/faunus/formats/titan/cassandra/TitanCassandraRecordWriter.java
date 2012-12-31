package com.thinkaurelius.faunus.formats.titan.cassandra;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.cassandra.thrift.Mutation;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {

    private final FaunusTitanCassandraGraph graph;
    private final RecordWriter<ByteBuffer, List<Mutation>> writer;

    public TitanCassandraRecordWriter(final FaunusTitanCassandraGraph graph, RecordWriter<ByteBuffer, List<Mutation>> writer) {
        this.graph = graph;
        this.writer = writer;
    }

    public void close(final TaskAttemptContext context) throws InterruptedException, IOException {
        this.writer.close(context);
    }

    public void write(final NullWritable key, final FaunusVertex value) throws InterruptedException, IOException {

        /*Mutation mut = new Mutation();
        Column c = new Column();
        c.setName(ByteBuffer.wrap(("col1").getBytes("UTF-8")));
        c.setValue(ByteBuffer.allocate(8).putLong(value.getIdAsLong()).array());
        c.setTimestamp(System.currentTimeMillis());
        ColumnOrSuperColumn t = new ColumnOrSuperColumn();
        t.setColumn(c);
        mut.setColumn_or_supercolumn(t);

        this.writer.write(ByteBuffer.wrap(("myval").getBytes("UTF-8")), Arrays.asList(mut));*/

        // TODO:
        //   - Note that if the keyspace doesn't exist, it is created (not so for HBase and table)

    }
}