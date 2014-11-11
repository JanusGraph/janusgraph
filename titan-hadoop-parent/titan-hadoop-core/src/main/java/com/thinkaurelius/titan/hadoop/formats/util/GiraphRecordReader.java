package com.thinkaurelius.titan.hadoop.formats.util;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphRecordReader extends RecordReader<NullWritable, GiraphComputeVertex> {

    private static final Logger log =
            LoggerFactory.getLogger(GiraphRecordReader.class);

    private RecordReader<StaticBuffer, Iterable<Entry>> reader;
    private TitanVertexDeserializer graph;
    private GiraphComputeVertex vertex;

    public GiraphRecordReader(final TitanVertexDeserializer graph, final RecordReader<StaticBuffer, Iterable<Entry>> reader) {
        this.graph = graph;
        this.reader = reader;
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (reader.nextKeyValue()) {
            // TODO titan05 integration -- the duplicate() call may be unnecessary
            final TinkerVertex maybeNullTinkerVertex =
                    graph.readHadoopVertex(reader.getCurrentKey(), reader.getCurrentValue());
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
        return vertex;
    }

    @Override
    public void close() throws IOException {
        graph.close();
        reader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }
}
