package com.thinkaurelius.faunus.formats.titan.hbase;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanHBaseRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {

    private final FaunusTitanHBaseGraph graph;
    private final RecordWriter<?, Mutation> writer;

    public TitanHBaseRecordWriter(final FaunusTitanHBaseGraph graph, final RecordWriter<?, Mutation> writer) {
        this.graph = graph;
        this.writer = writer;
    }

    public void close(final TaskAttemptContext context) throws InterruptedException, IOException {
        this.writer.close(context);
    }

    public void write(final NullWritable key, final FaunusVertex value) throws InterruptedException, IOException {
        // Below is just some made up put to ensure everything works

        // final Put put = new Put(ByteBuffer.allocate(8).putLong(value.getIdAsLong()).array());
        // put.add("fam1".getBytes(), "col1".getBytes(), ByteBuffer.allocate(8).putLong(value.getIdAsLong()).array());
        // this.writer.write(null, put);

        // TODO:
        //   Make use of the output.location.overwrite Faunus property to drop table?
        //   If table does not exist, create it (don't require use to HBase shell and create the table)
    }
}
