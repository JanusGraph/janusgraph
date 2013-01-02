package com.thinkaurelius.faunus.formats.titan.cassandra;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.cassandra.thrift.Mutation;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {

    private final FaunusTitanCassandraGraph graph;
    private final RecordWriter<ByteBuffer, List<Mutation>> writer;

    private final Map<Object, Long> vertexIds = new TreeMap<Object, Long>();
    private final Set<Object> processedEdges  = new HashSet<Object>();

    public TitanCassandraRecordWriter(final FaunusTitanCassandraGraph graph, RecordWriter<ByteBuffer, List<Mutation>> writer) {
        this.graph = graph;
        this.writer = writer;
    }

    public void close(final TaskAttemptContext context) throws InterruptedException, IOException {
        this.writer.close(context);
    }

    public void write(final NullWritable key, final FaunusVertex vertex) throws InterruptedException, IOException {
        TitanTransaction tx = graph.startTransaction();

        try {
            processVertex(vertex, tx);
        } catch (RuntimeException e) {
            tx.abort();
            throw e;
        } finally {
            tx.commit();
        }
    }

    private void processVertex(FaunusVertex vertex, TitanTransaction tx) {
        TitanVertex v = createOrGetTitanVertex(tx, vertex.getId());

        for (Map.Entry<String, Object> prop : vertex.getProperties().entrySet()) {
            tx.addProperty(v, prop.getKey(), prop.getValue());
        }

        storeEdges(vertex.getEdges(Direction.BOTH), tx);
    }

    private TitanVertex createOrGetTitanVertex(TitanTransaction tx, Object vertexId) {
        Long titanId = vertexIds.get(vertexId);

        if (titanId != null)
            return tx.getVertex(titanId.longValue());

        TitanVertex v = tx.addVertex();
        assert v != null : "Failed to create new Titan Vertex for Faunus ID: " + vertexId + ".";

        vertexIds.put(vertexId, v.getID());

        return v;
    }

    private void storeEdges(Iterable<Edge> edges, TitanTransaction tx) {
        for (Edge e : edges) {
            if (processedEdges.contains(e.getId())) // this edge has already been created
                continue;

            // make sure that vertices are allocated in Titan (properties are going to be added on 'processVertex' phrase)
            TitanVertex out = createOrGetTitanVertex(tx, e.getVertex(Direction.OUT).getId());
            TitanVertex in  = createOrGetTitanVertex(tx, e.getVertex(Direction.IN).getId());

            TitanEdge tEdge = tx.addEdge(out, in, e.getLabel());
            assert tEdge != null : "Failed to create new Titan Edge for Faunus Edge: " + e;
            processedEdges.add(e.getId());
        }
    }
}