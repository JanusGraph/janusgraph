package com.thinkaurelius.titan.testutil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.furnace.generators.DistributionGenerator;
import com.tinkerpop.furnace.generators.EdgeAnnotator;
import com.tinkerpop.furnace.generators.PowerLawDistribution;
import com.tinkerpop.furnace.generators.VertexAnnotator;

/**
 * Generates Titan graphs. Requires an open, empty Titan graph. Use a
 * {@link Builder} to create a generator instance. Open an empty Titan graph.
 * Finally, call {@link #generate(TitanGraph)} on it to fill it with generated
 * types and data.
 * <p>
 * Features:
 * 
 * <ul>
 * <li>Creating types</li>
 * <li>Inserting vertices and labeled, directed edges (power-law distribution)</li>
 * <li>Setting integer-valued, standard-indexed, out-unique vertex properties</li>
 * <li>Setting integer-valued, standard-indexed, out-unique edge properties</li>
 * </ul>
 * 
 * This uses a {@link java.util.Random} with a hard-coded seed. However, it also
 * uses {@link DistributionGenerator}, which in turn uses a {@code Random}
 * seeded with {@link System#currentTimeMillis()}. Because of this
 * time-sensitive seed in {@code DistributionGenerator}, the output of this
 * class is not sanely reproducible even when its inputs are fixed.
 */
public class GraphGenerator {

    public static final String UID_PROP = "uid";
    public static final long INITIAL_VERTEX_UID = 1L;
    public static long HIGH_DEG_VERTEX_UID = 0L;
    public static final double gamma = 2.5D;
    public static final String VERTEX_KEY_PREFIX = "vp_";
    public static final String EDGE_KEY_PREFIX = "ep_";
    public static final String LABEL_PREFIX = "el_";
    
    private static final int HIGH_DEG_INDEX = 0;
    
//    private final List<String> vProps;
//    private final List<String> eProps;

    private final int maxEdgePropVal;
    private final int maxVertexPropVal;
    /*
     * edgeCount must have type int instead of long because
     * DistributionGenerator expects int. It's probably not a great idea to go
     * over 4B per label in memory anyway.
     */
    private final int edgeCount;
    private final int vertexCount;
    private final int vertexPropKeys;
    private final int edgePropKeys;
    private final int edgeLabels;
    private final String[] vertexPropNames;
    private final String[] edgePropNames;
    private final String[] edgeLabelNames;
    private final Map<String, String> labelPkeys;

    private final Random random = new Random(64);
    private long uidCounter = INITIAL_VERTEX_UID; // uses initial value and increments
    
    /**
     * Construct {@link GraphGenerator} using the {@code Builder} pattern.
     * 
     * @see GraphGenerator
     */
    public static class Builder {
        
        private int maxVertexPropVal = 100;
        private int maxEdgePropVal = 100;
        private int vertexPropKeys = 20;
        private int edgePropKeys = 10;
        private int edgeLabels = 3;
        private int vertexCount = -1;
        private int edgeCount = -1;
        
        /**
         * Set the maximum value of vertex properties. This is an exclusive
         * limit. The minimum is always 0.
         * 
         * @param m
         *            maximum vertex property value, exclusive
         * @return self
         */
        public Builder setMaxVertexPropVal(int m) {
            maxVertexPropVal = m;
            return this;
        }
        
        /**
         * Set the maximum value of edge properties. This is an exclusive limit.
         * The minimum is always 0.
         * 
         * @param m
         *            maximum edge property value, exclusive
         * @return self
         */
        public Builder setMaxEdgePropVal(int m) {
            maxEdgePropVal = m;
            return this;
        }

        /**
         * Set the total number of distinct property keys to use for vertex
         * properties.
         * 
         * @param vertexPropKeys
         *            number of property keys
         * @return self
         */
        public Builder setVertexPropKeys(int vertexPropKeys) {
            this.vertexPropKeys = vertexPropKeys;
            return this;
        }

        /**
         * Set the total number of distinct property keys to use for edge
         * properties.
         * 
         * @param edgePropKeys
         *            number of property keys
         * @return self
         */
        public Builder setEdgePropKeys(int edgePropKeys) {
            this.edgePropKeys = edgePropKeys;
            return this;
        }

        /**
         * Set the total number of edge labels to create.
         * 
         * @param edgeLabels
         *            number of edge labels
         * @return self
         */
        public Builder setEdgeLabels(int edgeLabels) {
            this.edgeLabels = edgeLabels;
            return this;
        }

        /**
         * Set the number of vertices to create.
         * 
         * @param vertexCount
         *            global vertex total
         * @return self
         */
        public Builder setVertexCount(int vertexCount) {
            this.vertexCount = vertexCount;
            Preconditions.checkArgument(0 <= this.vertexCount);
            return this;
        }

        /**
         * Set the number of edges to create for each edge label.
         * 
         * @param edgeCount
         *            global edge total for each label
         * @return self
         */
        public Builder setEdgeCount(int edgeCount) {
            this.edgeCount = edgeCount;
            Preconditions.checkArgument(0 <= this.edgeCount);
            return this;
        }
        
        public Builder(int vertexCount, int edgeCount) {
            setVertexCount(vertexCount);
            setEdgeCount(edgeCount);
        }
        
        /**
         * Construct a {@link GraphGenerator} with this {@code Builder}'s
         * settings.
         * 
         * @return a new GraphGenerator
         */
        public GraphGenerator build() {
            return new GraphGenerator(maxEdgePropVal, maxVertexPropVal,
                    vertexCount, edgeCount, vertexPropKeys, edgePropKeys,
                    edgeLabels);
        }
    }
    
    public void generate(TitanGraph g) {
        makeTypes(g);
        addVerticesAndEdges(g);
    }
    
    /**
     * Use the {@link Builder}.
     * 
     * @see GraphGenerator.Builder
     */
    private GraphGenerator(int maxEdgePropVal, int maxVertexPropVal,
            int vertexCount, int edgeCount, int vertexPropKeys,
            int edgePropKeys, int edgeLabels) {
        super();
        this.maxEdgePropVal = maxEdgePropVal;
        this.maxVertexPropVal = maxVertexPropVal;
        this.vertexCount = vertexCount;
        this.edgeCount = edgeCount;
        this.vertexPropKeys = vertexPropKeys;
        this.edgePropKeys = edgePropKeys;
        this.edgeLabels = edgeLabels;
        
        this.vertexPropNames = generateNames(VERTEX_KEY_PREFIX, this.vertexPropKeys);
        this.edgePropNames = generateNames(EDGE_KEY_PREFIX, this.edgePropKeys);
        this.edgeLabelNames = generateNames(LABEL_PREFIX, this.edgeLabels);
        
        Preconditions.checkArgument(this.edgeLabels <= this.edgePropKeys);
        
        this.labelPkeys = new HashMap<String, String>(this.edgeLabels);
        for (int i = 0; i < this.edgeLabels; i++) {
            labelPkeys.put(edgeLabelNames[i], edgePropNames[i]);
        }
    }
    
    private String[] generateNames(String prefix, int count) {
        String[] result = new String[count];
        StringBuilder sb = new StringBuilder(8);
        sb.append(prefix);
        for (int i = 0; i < count; i++) {
            sb.append(i);
            result[i] = sb.toString();
            sb.setLength(prefix.length());
        }
        return result;
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
            vertex.setProperty(UID_PROP, uidCounter++);
            int count  = random.nextInt(vertexPropKeys);
            int offset = random.nextInt(vertexPropKeys);
            int length = vertexPropNames.length;
            for (int i = 0; i < count; i++) {
                String key = vertexPropNames[(i + offset) % length];
                int value = random.nextInt(maxVertexPropVal);
                vertex.setProperty(key, value);
            }
        }
    };
    
    private final EdgeAnnotator edgeAnnotator = new EdgeAnnotator() {
        @Override
        public void annotate(Edge edge) {
            // Set primary key edge property
            edge.setProperty(labelPkeys.get(edge.getLabel()),
                             random.nextInt(maxEdgePropVal));
            
            // Set additional (non-primary-key) edge properties
            if (0 >= edgePropKeys - edgeLabels)
                return;
            
            Preconditions.checkArgument(edgePropKeys == edgePropNames.length);
            int eligible = edgePropKeys - edgeLabels;
            Preconditions.checkArgument(0 < eligible);
            int count  = random.nextInt(eligible);
            int offset = random.nextInt(eligible);
            int length = edgePropNames.length;
            Preconditions.checkArgument(length == eligible + edgeLabels);
            Preconditions.checkArgument(length >= count    + edgeLabels);
            
            for (int i = 0; i < count; i++) {
                String key = edgePropNames[edgeLabels + ((offset + i) % eligible)];
                edge.setProperty(key, random.nextInt(maxEdgePropVal));
            }
        }
    };
    
    public final String getVertexPropertyName(int i) {
        return vertexPropNames[i];
    }
    
    public final String getEdgePropertyName(int i) {
        return edgePropNames[i];
    }
    
    public final String getEdgeLabelName(int i) {
        return edgeLabelNames[i];
    }
    
    public final String getPrimaryKeyForLabel(String l) {
        return l.replace("el_", "ep_");
    }
    
    public final long getHighDegVertexUid() {
        return HIGH_DEG_VERTEX_UID;
    }
    
    public final String getHighDegEdgeLabel() {
        return getEdgeLabelName(HIGH_DEG_INDEX);
    }
    
    public final long getMaxUid() {
        return uidCounter;
    }
    
    public int getVertexPropKeys() {
        return vertexPropKeys;
    }

    public int getEdgePropKeys() {
        return edgePropKeys;
    }

    public int getMaxEdgePropVal() {
        return maxEdgePropVal;
    }

    public int getMaxVertexPropVal() {
        return maxVertexPropVal;
    }

    /**
     * Generate a scale-free graph of the size specified by the arguments.
     * 
     * @return A graph whose in- and out-degree distributions follow a power
     *         law.
     */
    public void addVerticesAndEdges(TitanGraph g) {
        
        // Generate vertices
        for (long i = uidCounter; i < vertexCount + INITIAL_VERTEX_UID - 1; i++) {
            Vertex v = g.addVertex(i);
            // DistributionGenerator doesn't currently support VertexAnnotator
            vertexAnnotator.annotate(v, null);
        }
        
        // Generate edges
        for (int i = 0; i < edgeLabels; i++) {
            DistributionGenerator gen = new DistributionGenerator(getEdgeLabelName(i), edgeAnnotator);
            gen.setOutDistribution(new PowerLawDistribution(gamma));
            gen.setInDistribution(new PowerLawDistribution(gamma));
            gen.generate(g, edgeCount);
        }
        
        g.commit();
        
        TitanTransaction tx = g.newTransaction();
        // Add a vertex that has an out edge to every other vertex
        Vertex hiOutDeg = tx.addVertex(HIGH_DEG_VERTEX_UID);
        String label = getHighDegEdgeLabel();
        TitanKey uidKey = tx.getPropertyKey(UID_PROP);
        hiOutDeg.setProperty(UID_PROP, HIGH_DEG_VERTEX_UID);
        String pKey = getPrimaryKeyForLabel(label);
        for (long i = INITIAL_VERTEX_UID; i < vertexCount; i++) {
            Vertex in = tx.getVertex(uidKey, i);
            Edge e = hiOutDeg.addEdge(label, in);
            e.setProperty(pKey, (int)i);
        }
        
        tx.commit();
    }
    
    public void makeTypes(TitanGraph g) {
        Preconditions.checkArgument(edgeLabels <= edgePropKeys);
        
        TitanTransaction tx = g.newTransaction();
        for (int i = 0; i < vertexPropKeys; i++) {
            tx.makeType().name(getVertexPropertyName(i)).dataType(Integer.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();
        }
        for (int i = 0; i < edgePropKeys; i++) {
            tx.makeType().name(getEdgePropertyName(i)).dataType(Integer.class).indexed(Edge.class).unique(Direction.OUT).makePropertyKey();
        }
        for (int i = 0; i < edgeLabels; i++) {
            String labelName = getEdgeLabelName(i);
            String pkName = getPrimaryKeyForLabel(labelName);
            TitanKey pk = tx.getPropertyKey(pkName);
            tx.makeType().name(getEdgeLabelName(i)).primaryKey(pk).makeEdgeLabel();
        }

        tx.makeType().name(UID_PROP).dataType(Long.class).indexed(Vertex.class).unique(Direction.BOTH).makePropertyKey();
        tx.commit();
    }
}
