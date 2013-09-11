package com.thinkaurelius.titan.testutil;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLWriter;
import com.tinkerpop.furnace.generators.DistributionGenerator;
import com.tinkerpop.furnace.generators.EdgeAnnotator;
import com.tinkerpop.furnace.generators.PowerLawDistribution;
import com.tinkerpop.furnace.generators.VertexAnnotator;

public class GraphGenerator {

    public static final String UID_PROP = "uid";

    public static final int MAX_EDGE_PROP_VALUE = 100; // exclusive
    public static final int MAX_VERTEX_PROP_VALUE = 100; // exclusive

    private final Random random = new Random(64);
    
    private final double gamma = 2.5D;
    
    private final List<String> vProps;
    private final List<String> eProps;
    private long uidCounter = 0L;
    
    public static void main(String args[]) throws IOException {
        writeDefaultGraph();
    }
    
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
            int count = random.nextInt(eProps.size());
            for (String key : choose(eProps, count)) {
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
    
    public final long getMaxUid() {
        return uidCounter;
    }
    
    /**
     * Generate a scale-free graph of the size specified by the arguments.
     * 
     * @return A graph whose in- and out-degree distributions follow a power
     *         law.
     */
    public void generateScaleFreeGraph(Graph g, int vertexCount, int edgeCount) {
        for (int i = 0; i < vertexCount; i++) {
            Vertex v = g.addVertex(i);
            // DistributionGenerator doesn't currently support VertexAnnotator
            vertexAnnotator.annotate(v, null);
        }
        DistributionGenerator gen = new DistributionGenerator("el_0", edgeAnnotator);
        gen.setOutDistribution(new PowerLawDistribution(gamma));
        gen.setInDistribution(new PowerLawDistribution(gamma));
        gen.generate(g, edgeCount);
    }
    
    public void generateScaleFreeTitanGraph(TitanGraph g, int vertexCount, int edgeCount) {
        TitanTransaction tx = g.newTransaction();
        for (int i = 0; i < vProps.size(); i++) {
            tx.makeType().name(getVertexPropertyName(i)).dataType(Integer.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();
        }
        for (int i = 0; i < eProps.size(); i++) {
            tx.makeType().name(getEdgePropertyName(i)).dataType(Integer.class).indexed(Edge.class).unique(Direction.OUT).makePropertyKey();
        }

        tx.makeType().name(UID_PROP).dataType(Long.class).indexed(Vertex.class).unique(Direction.BOTH).makePropertyKey();
        tx.commit();
        generateScaleFreeGraph(g, vertexCount, edgeCount);
        g.commit();
    }
    
    /**
     * Write a TinkerGraph in GraphML format to a file.
     * 
     * @param g graph to output
     * @param filename destination filename
     * @throws IOException on write error
     */
    public void writeTinkerGraphML(TinkerGraph g, String filename) throws IOException {
        GraphMLWriter writer = new GraphMLWriter(g);
        writer.outputGraph(filename);
    }
    
    /**
     * Write a scale-free graph with 10k vertices and 100k edges to test.graphml
     * in the current working directory.
     * 
     * @throws IOException on write error
     */
    public static void writeDefaultGraph() throws IOException {
        int vc =  10 * 1000;
        int ec = 100 * 1000;
        String f = "test.graphml";
        GraphGenerator gen = new GraphGenerator(20, 10);
        TinkerGraph graph = new TinkerGraph();
        gen.generateScaleFreeGraph(graph, vc, ec);
        gen.writeTinkerGraphML(graph, f);
        System.out.println(String.format("Wrote %d vertices and %d edges to %s", ec, vc, f));
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
