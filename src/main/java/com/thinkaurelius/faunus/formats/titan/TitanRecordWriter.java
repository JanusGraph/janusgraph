package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;

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
    public final static String FAUNUS_IDX_ID = "_faunusID";

    private final StandardTitanGraph graph;

    public TitanRecordWriter(final StandardTitanGraph graph) {
        this.graph = graph;
    }

    public void close(final TaskAttemptContext context) throws InterruptedException, IOException {
    }

    public void write(final NullWritable key, final FaunusVertex faunusVertex) throws InterruptedException, IOException {
        try {
            this.processVertex(faunusVertex);
            graph.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            graph.stopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new InterruptedException(e.getMessage());
        }
    }

    private void processVertex(final FaunusVertex faunusVertex) {
        Vertex titanVertex = createOrGetTitanVertex(faunusVertex.getIdAsLong());
        // need the vertex alive for the new transaction could just have started
        titanVertex = this.graph.getVertex(titanVertex);
        // copy existing properties over to the TitanVertex
        for (final Map.Entry<String, Object> property : faunusVertex.getProperties().entrySet()) {
            titanVertex.setProperty(property.getKey(), property.getValue());
        }
        storeEdges(titanVertex, faunusVertex.getEdges(Direction.OUT));
    }

    private Vertex createOrGetTitanVertex(final Long faunusVertexId) {
        Vertex titanVertex = this.getTitanVertexViaFaunusId(faunusVertexId);
        if (titanVertex != null) {
            return titanVertex;
        } else {
            titanVertex = this.graph.addVertex(null);
            assert titanVertex != null : "Failed to create new TitanVertex for Faunus ID: " + faunusVertexId + ".";
            // add a special Faunus property with makes vertex recognizable by other reducers
            titanVertex.setProperty(FAUNUS_IDX_ID, faunusVertexId);
            // immediately commit so available to other mappers
            this.graph.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
            return titanVertex;
        }
    }

    private void storeEdges(final Vertex titanVertex, final Iterable<Edge> edges) {
        for (final Edge faunusEdge : edges) {
            // re-getVertex() -- stupid hack to make sure the transactions are still alive for these vertices
            // transaction can be successfully committed during createOrGetTitanVertex
            final Vertex inVertex = this.graph.getVertex(createOrGetTitanVertex((Long) faunusEdge.getVertex(Direction.IN).getId()));
            final Vertex outVertex = this.graph.getVertex(titanVertex);
            final Edge titanEdge = this.graph.addEdge(null, outVertex, inVertex, faunusEdge.getLabel());
            for (final String key : faunusEdge.getPropertyKeys()) {
                titanEdge.setProperty(key, faunusEdge.getProperty(key));
            }
            assert titanEdge != null : "Failed to create new TitanEdge for FaunusEdge: " + faunusEdge;
        }
    }

    private Vertex getTitanVertexViaFaunusId(final Long vertexId) {
        Vertex titanVertex = null;
        int count = 0;
        for (final Vertex v : this.graph.getVertices(FAUNUS_IDX_ID, vertexId)) {
            // just a sanity check because IDs are guaranteed to be unique
            assert count <= 1 : FAUNUS_IDX_ID + " should be unique";
            titanVertex = v;
            count++;
        }
        return titanVertex;
    }
}