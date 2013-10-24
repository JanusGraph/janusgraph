package com.thinkaurelius.titan.testutil.gen;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLWriter;
import com.tinkerpop.furnace.alpha.generators.DistributionGenerator;
import com.tinkerpop.furnace.alpha.generators.EdgeAnnotator;
import com.tinkerpop.furnace.alpha.generators.PowerLawDistribution;
import com.tinkerpop.furnace.alpha.generators.VertexAnnotator;

/**
 * Generates Titan graphs. Requires an open, empty Titan graph.
 * <p/>
 * Features:
 * <p/>
 * <ul>
 * <li>Creating types</li>
 * <li>Inserting vertices and labeled, directed edges (power-law distribution)</li>
 * <li>Setting integer-valued, standard-indexed, single-valued properties on edges and vertices</li>
 * </ul>
 * <p/>
 * This uses a {@link java.util.Random} with a hard-coded seed. However, it also
 * uses {@link DistributionGenerator}, which in turn uses a {@code Random}
 * seeded with {@link System#currentTimeMillis()}. Because of this
 * time-sensitive seed in {@code DistributionGenerator}, the output of this
 * class is not sanely reproducible even when its inputs are fixed.
 */
public class GraphGenerator {

    public static final long INITIAL_VERTEX_UID = 1L;
    public static final double GAMMA = 2.5D;

    private final Schema schema;

    private final Random random = new Random(64);
    private long uidCounter = INITIAL_VERTEX_UID; // uses initial value and increments

    public GraphGenerator(Schema schema) {
        this.schema = schema;
    }

    public void generateTypesAndData(TitanGraph g) {
        schema.makeTypes(g);
        generateData(g);
    }

    /**
     * Generate a scale-free graph using parameters specified in the
     * {@link Schema} object provided to this instance's constructor. The types
     * must already exist. The types can be created by either calling
     * {@link #generateTypesAndData(TitanGraph)} instead of this method or by
     * calling {@link Schema#makeTypes(TitanGraph)} before calling this method.
     *
     * @param g A graph with types predefined. If it already contains vertices
     *          or relations, they may be overwritten.
     */
    public void generateData(TitanGraph g) {

        // Generate vertices
        for (long i = uidCounter; i < schema.getVertexCount() + INITIAL_VERTEX_UID - 1; i++) {
            Vertex v = g.addVertex(i);
            // DistributionGenerator doesn't currently support VertexAnnotator
            vertexAnnotator.annotate(v, null);
        }

        // Generate edges
        for (int i = 0; i < schema.getEdgeLabels(); i++) {
            DistributionGenerator gen = new DistributionGenerator(schema.getEdgeLabelName(i), edgeAnnotator);
            gen.setOutDistribution(new PowerLawDistribution(GAMMA));
            gen.setInDistribution(new PowerLawDistribution(GAMMA));
            gen.generate(g, schema.getEdgeCount());
        }

        g.commit();

        TitanTransaction tx = g.newTransaction();
        // Add a vertex that has an out edge to every other vertex
        Vertex hiOutDeg = tx.addVertex(Schema.SUPERNODE_UID);
        String label = schema.getSupernodeOutLabel();
        TitanKey uidKey = tx.getPropertyKey(Schema.UID_PROP);
        hiOutDeg.setProperty(Schema.UID_PROP, Schema.SUPERNODE_UID);
        String pKey = schema.getSortKeyForLabel(label);
        for (long i = INITIAL_VERTEX_UID; i < schema.getVertexCount(); i++) {
            Vertex in = tx.getVertex(uidKey, i);
            Edge e = hiOutDeg.addEdge(label, in);
            e.setProperty(pKey, (int) i);
        }

        tx.commit();
    }

    public static void writeData(TitanGraph g, OutputStream data) throws IOException {
        GraphMLWriter.outputGraph(g, data);
    }

    public void generateTypesAndLoadData(TitanGraph g, InputStream data) throws IOException {
        schema.makeTypes(g);
        GraphMLReader.inputGraph(g, data);
    }

    public static void main(String args[]) throws IOException {
        if (4 != args.length) {
            System.err.println("Usage: GraphGenerator titanproperties vertexcount edgecount outfile");
            System.exit(1);
        }
        int i = 0;
        String c = args[i++];
        int v = Integer.valueOf(args[i++]);
        int e = Integer.valueOf(args[i++]);
        String o = args[i++];
        Schema s = new Schema.Builder(v, e).build();
        TitanGraph graph = TitanFactory.open(c);
        new GraphGenerator(s).generateTypesAndData(graph);
        OutputStream out = new GZIPOutputStream(new FileOutputStream(o));
        GraphGenerator.writeData(graph, out);
        out.close();
        graph.shutdown();
        System.exit(0);
    }

    private final VertexAnnotator vertexAnnotator = new VertexAnnotator() {

        /**
         * Choose a sequence of contiguously-stored property names from
         * {@link GraphGenerator#vertexPropNames} with random offset and length
         * (possibly wrapping around the end of the array). The length (i.e. the
         * number of property keys chosen) is on
         * {@code [0, GraphGenerator#vertexPropKeys]}. Set the value of each
         * chosen property key to a random value on
         * {@code [0, GraphGenerator#maxVertexPropVal)}
         */
        @Override
        public void annotate(Vertex vertex, Map<String, Object> context) {
            vertex.setProperty(Schema.UID_PROP, uidCounter++);
            int length = schema.getVertexPropKeys();
            int count = random.nextInt(length);
            int offset = random.nextInt(length);
            for (int i = 0; i < count; i++) {
                String key = schema.getVertexPropertyName((i + offset) % length);
                int value = random.nextInt(schema.getMaxVertexPropVal());
                vertex.setProperty(key, value);
            }
        }
    };

    private final EdgeAnnotator edgeAnnotator = new EdgeAnnotator() {

        /**
         * As in {@code vertexAnnotator} above, choose a sequence of
         * contiguously-stored property names with a random offset and length.
         * However, omit the initial {@link Schema#getEdgeLabels()} property
         * keys from consideration. These keys are used by sort keys.
         * <p>
         * In addition to the preceding, lookup the sort key for the edge's
         * label and set that to a randomly chosen value.
         */
        @Override
        public void annotate(Edge edge) {
            // Set sort key edge property
            edge.setProperty(schema.getSortKeyForLabel(edge.getLabel()),
                    random.nextInt(schema.getMaxEdgePropVal()));

            // Set additional (non-sort-key) edge properties
            if (0 >= schema.getEdgePropKeys() - schema.getEdgeLabels())
                return;

            int eligible = schema.getEdgePropKeys() - schema.getEdgeLabels();
            Preconditions.checkArgument(0 < eligible);
            int count = random.nextInt(eligible);
            int offset = random.nextInt(eligible);
            int length = schema.getEdgePropKeys();
            Preconditions.checkArgument(length == eligible + schema.getEdgeLabels());
            Preconditions.checkArgument(length >= count + schema.getEdgeLabels());

            for (int i = 0; i < count; i++) {
                String key = schema.getEdgePropertyName(schema.getEdgeLabels() + ((offset + i) % eligible));
                edge.setProperty(key, random.nextInt(schema.getMaxEdgePropVal()));
            }
        }
    };
}
