package com.thinkaurelius.titan.hadoop.formats.hbase;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanHBaseRecordReader extends RecordReader<NullWritable, GiraphComputeVertex> {

    private TableRecordReader reader;
    private TitanHBaseHadoopGraph graph;
    //private Configuration configuration;
    private GiraphComputeVertex vertex;

    private final byte[] edgestoreFamilyBytes;

    public TitanHBaseRecordReader(final TitanHBaseHadoopGraph graph, final TableRecordReader reader, final byte[] edgestoreFamilyBytes) {
        this.graph = graph;
        this.reader = reader;
        this.edgestoreFamilyBytes = edgestoreFamilyBytes;
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);
        //configuration = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(taskAttemptContext));
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (reader.nextKeyValue()) {
            final byte[] rawKey = reader.getCurrentKey().copyBytes();
            final TinkerVertex maybeNullTinkerVertex =
                    graph.readHadoopVertex(rawKey, reader.getCurrentValue().getMap().get(edgestoreFamilyBytes));
            if (null != maybeNullTinkerVertex) {
                vertex = new GiraphComputeVertex(maybeNullTinkerVertex);
                //vertexQuery.filterRelationsOf(vertex); // TODO reimplement vertexquery filtering
                return true;
            }
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public GiraphComputeVertex getCurrentValue() throws IOException, InterruptedException {
        return this.vertex;
    }

    @Override
    public void close() throws IOException {
        this.graph.close();
        this.reader.close();
    }

    @Override
    public float getProgress() {
        return this.reader.getProgress();
    }
}
