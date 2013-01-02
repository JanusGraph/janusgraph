package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author Pavel Yaskevich
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {

    private final StandardTitanGraph graph;

    private final Map<Object, Long> vertexIds = new TreeMap<Object, Long>();
    private final Set<Object> processedEdges = new HashSet<Object>();

    public TitanRecordWriter(final StandardTitanGraph graph) {
        this.graph = graph;
    }

    public void close(final TaskAttemptContext context) throws InterruptedException, IOException {
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

    private void processVertex(final FaunusVertex vertex, final TitanTransaction tx) {
        TitanVertex v = createOrGetTitanVertex(tx, vertex.getId());

        for (Map.Entry<String, Object> prop : vertex.getProperties().entrySet()) {
            tx.addProperty(v, prop.getKey(), prop.getValue());
        }

        storeEdges(vertex.getEdges(Direction.BOTH), tx);
    }

    private TitanVertex createOrGetTitanVertex(final TitanTransaction tx, final Object vertexId) {
        final Long titanId = vertexIds.get(vertexId);

        if (titanId != null)
            return tx.getVertex(titanId.longValue());

        TitanVertex v = tx.addVertex();
        assert v != null : "Failed to create new TitanVertex for Faunus ID: " + vertexId + ".";

        vertexIds.put(vertexId, v.getID());

        return v;
    }

    private void storeEdges(final Iterable<Edge> edges, final TitanTransaction tx) {
        for (final Edge e : edges) {
            if (processedEdges.contains(e.getId())) // this edge has already been created
                continue;

            // make sure that vertices are allocated in Titan (properties are going to be added on 'processVertex' phrase)
            TitanVertex out = createOrGetTitanVertex(tx, e.getVertex(Direction.OUT).getId());
            TitanVertex in = createOrGetTitanVertex(tx, e.getVertex(Direction.IN).getId());

            TitanEdge tEdge = tx.addEdge(out, in, e.getLabel());
            assert tEdge != null : "Failed to create new TitanEdge for FaunusEdge: " + e;
            processedEdges.add(e.getId());
        }
    }
}