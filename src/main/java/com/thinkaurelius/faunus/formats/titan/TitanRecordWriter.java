package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

/**
 * @author Pavel Yaskevich
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {
    /**
     * Special internal property (index) which allows to find vertices in Titan by Faunus ID,
     * which is required for writer to be operational as map/reduce jobs could be run on different physical
     * machines and there is no way to establish mapping from Faunus-ID to Titan-ID without using this mechanism.
     */
    private final static String FAUNUS_IDX_ID = "_faunusID";

    private final StandardTitanGraph graph;

    public TitanRecordWriter(final StandardTitanGraph graph) {
        this.graph = graph;

        TitanTransaction tx = graph.startTransaction();
        tx.createKeyIndex(FAUNUS_IDX_ID, Vertex.class);
        tx.commit();
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

        // copy existing properties over to the TitanVertex (register in Titan)
        for (Map.Entry<String, Object> prop : vertex.getProperties().entrySet()) {
            tx.addProperty(v, prop.getKey(), prop.getValue());
        }

        // add a special Faunus property with makes vertex recognizable by other reducers
        tx.addProperty(v, FAUNUS_IDX_ID, vertex.getIdAsLong());

        storeEdges(vertex.getEdges(Direction.BOTH), tx);
    }

    private TitanVertex createOrGetTitanVertex(final TitanTransaction tx, final Object vertexId) {
        TitanVertex v = getVertex(tx, vertexId);
        if (v != null)
            return v;

        v = tx.addVertex();
        assert v != null : "Failed to create new TitanVertex for Faunus ID: " + vertexId + ".";

        return v;
    }

    private void storeEdges(final Iterable<Edge> edges, final TitanTransaction tx) {
        for (final Edge e : edges) {
            TitanVertex out = getVertex(tx, e.getVertex(Direction.OUT).getId());
            TitanVertex in  = getVertex(tx, e.getVertex(Direction.IN).getId());

            // edge should only be created when both vertices are present (hence it's created by last vertex of the pair)
            if (out == null || in == null)
                continue;

            TitanEdge tEdge = tx.addEdge(out, in, e.getLabel());
            assert tEdge != null : "Failed to create new TitanEdge for FaunusEdge: " + e;
        }
    }

    private static TitanVertex getVertex(TitanTransaction tx, Object vertexId) {
        TitanVertex v = null;

        int count = 0;
        for (Vertex tV : tx.getVertices(FAUNUS_IDX_ID, vertexId)) {
            // just a sanity check because IDs are guaranteed to be unique
            assert count <= 1 : FAUNUS_IDX_ID + " should be unique";
            v = (TitanVertex) tV;
            count++;
        }

        return v;
    }
}