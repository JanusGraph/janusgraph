package com.thinkaurelius.titan.testutil;

import java.util.ArrayList;
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

public class GraphGenerator {

    public static final String UID_PROP = "uid";

    public static final int MAX_EDGE_PROP_VALUE = 100; // exclusive
    public static final int MAX_VERTEX_PROP_VALUE = 100; // exclusive
    public static final long INITIAL_VERTEX_UID = 1L;
    
    private final Random random = new Random(64);
    
    private final double gamma = 2.5D;
    
    private final List<String> vProps;
    private final List<String> eProps;
    private final int eLabelCount = 3;
    private long highDegVertexUid = 0L;
    private int highDegVertexEdgeLabelIndex = 0;
    private long uidCounter = INITIAL_VERTEX_UID; // uses initial value and increments
    
    private final VertexAnnotator vertexAnnotator = new VertexAnnotator() {
        @Override
        public void annotate(Vertex vertex, Map<String, Object> context) {
            int count = random.nextInt(vProps.size());
            for (String key : choose(vProps, count)) {
                vertex.setProperty(key, random.nextInt(MAX_VERTEX_PROP_VALUE));
            }
            vertex.setProperty(UID_PROP, uidCounter++);
        }
    };
    
    private final EdgeAnnotator edgeAnnotator = new EdgeAnnotator() {
        @Override
        public void annotate(Edge edge) {
            // Set primary key edge property
            String label = edge.getLabel();
            String labelPk = getPrimaryKeyForLabel(label);
            edge.setProperty(labelPk, random.nextInt(MAX_EDGE_PROP_VALUE));
            
            // Set additional (non-primary-key) edge properties
            List<String> nonPks = new ArrayList<String>(eProps.size() - 1);
            for (String edgePropName : eProps) {
                if (!edgePropName.equals(labelPk)) {
                    nonPks.add(edgePropName);
                }
            }
            int count = random.nextInt(nonPks.size());
            for (String key : choose(nonPks, count)) {
                edge.setProperty(key, random.nextInt(MAX_EDGE_PROP_VALUE));
            }
        }
    };
    
    public GraphGenerator(int vertexProperties, int edgeProperties) {
        ImmutableSet.Builder<String> b;
        
        b = ImmutableSet.builder();
        for (int i = 0; i < vertexProperties; i++) {
            b.add(getVertexPropertyName(i));
        }
        vProps = ImmutableList.copyOf(b.build());
        
        b = ImmutableSet.builder();
        for (int i = 0; i < edgeProperties; i++) {
            b.add(getEdgePropertyName(i));
        }
        eProps = ImmutableList.copyOf(b.build());
    }
    
    public final String getVertexPropertyName(int i) {
        return String.format("vp_%d", i);
    }
    
    public final String getEdgePropertyName(int i) {
        return String.format("ep_%d", i);
    }
    
    public final String getEdgeLabelName(int i) {
        return String.format("el_%d", i);
    }
    
    public final String getPrimaryKeyForLabel(String l) {
        return l.replace("el_", "ep_");
    }
    
    public final long getHighDegVertexUid() {
        return highDegVertexUid;
    }
    
    public final String getHighDegEdgeLabel() {
        return getEdgeLabelName(highDegVertexEdgeLabelIndex);
    }
    
    public final String getHighDegEdgeProp() {
        return getPrimaryKeyForLabel(getHighDegEdgeLabel());
    }
    
    public final long getMaxUid() {
        return uidCounter;
    }
    
    /**
     * Generate a scale-free graph of the size specified by the arguments.
     * 
     * @return A graph whose in- and out-degree distributions follow a power
     *         law.
     */
    public void addVerticesAndEdges(TitanGraph g, int vertexCount, int edgeCount) {
        
        // Generate vertices
        for (long i = uidCounter; i < vertexCount + INITIAL_VERTEX_UID - 1; i++) {
            Vertex v = g.addVertex(i);
            // DistributionGenerator doesn't currently support VertexAnnotator
            vertexAnnotator.annotate(v, null);
        }
        
        // Generate edges
        for (int i = 0; i < eLabelCount; i++) {
            DistributionGenerator gen = new DistributionGenerator(getEdgeLabelName(i), edgeAnnotator);
            gen.setOutDistribution(new PowerLawDistribution(gamma));
            gen.setInDistribution(new PowerLawDistribution(gamma));
            gen.generate(g, edgeCount);
        }
        
        g.commit();
        
        TitanTransaction tx = g.newTransaction();
        // Add a vertex that has an out edge to every other vertex
        Vertex hiOutDeg = tx.addVertex(highDegVertexUid);
        String label = getHighDegEdgeLabel();
        TitanKey uidKey = tx.getPropertyKey(UID_PROP);
        hiOutDeg.setProperty(UID_PROP, highDegVertexUid);
        String pKey = getPrimaryKeyForLabel(label);
        for (long i = INITIAL_VERTEX_UID; i < vertexCount + INITIAL_VERTEX_UID - 1; i++) {
            Vertex in = tx.getVertex(uidKey, i);
            Edge e = hiOutDeg.addEdge(label, in);
            e.setProperty(pKey, (int)i);
        }
        
        tx.commit();
    }
    
    public void makeTypes(TitanGraph g) {
        Preconditions.checkArgument(eLabelCount <= eProps.size());
        
        TitanTransaction tx = g.newTransaction();
        for (int i = 0; i < vProps.size(); i++) {
            tx.makeType().name(getVertexPropertyName(i)).dataType(Integer.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();
        }
        for (int i = 0; i < eProps.size(); i++) {
            tx.makeType().name(getEdgePropertyName(i)).dataType(Integer.class).indexed(Edge.class).unique(Direction.OUT).makePropertyKey();
        }
        for (int i = 0; i < eLabelCount; i++) {
            String labelName = getEdgeLabelName(i);
            String pkName = getPrimaryKeyForLabel(labelName);
            TitanKey pk = tx.getPropertyKey(pkName);
            tx.makeType().name(getEdgeLabelName(i)).primaryKey(pk).makeEdgeLabel();
        }

        tx.makeType().name(UID_PROP).dataType(Long.class).indexed(Vertex.class).unique(Direction.BOTH).makePropertyKey();
        tx.commit();
    }
    
    public void generate(TitanGraph g, int vertexCount, int edgeCount) {
        makeTypes(g);
        addVerticesAndEdges(g, vertexCount, edgeCount);
    }
    
    private final List<String> choose(List<String> elems, int count) {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        Set<Integer> visited = new HashSet<Integer>(count);
        int collected = 0;
        while (collected < count) {
            int index = random.nextInt(elems.size());
            if (visited.contains(index))
                continue;
            
            builder.add(elems.get(index));
            visited.add(index);
            collected++;
        }
        return builder.build();
    }
}
