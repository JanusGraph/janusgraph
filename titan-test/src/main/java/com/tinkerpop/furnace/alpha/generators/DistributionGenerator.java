package com.tinkerpop.furnace.alpha.generators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

/**
 * Generates a synthetic network for a given out- and (optionally) in-degree distribution.
 * 
 * After construction, at least the out-degree distribution must be set via {@link #setOutDistribution}
 * 
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class DistributionGenerator extends AbstractGenerator {

    private Distribution outDistribution;
    private Distribution inDistribution;

    private boolean allowLoops = true;

    /**
     * 
     * @param label
     * @param annotator
     * @see AbstractGenerator#AbstractGenerator(String, EdgeAnnotator) 
     */
    public DistributionGenerator(String label, EdgeAnnotator annotator) {
        super(label,annotator);
    }

    /**
     * 
     * @param label
     * @see AbstractGenerator#AbstractGenerator(String) 
     */
    public DistributionGenerator(String label) {
        super(label);
    }

    /**
     * Sets the out-degree distribution to be used by this generator. 
     * 
     * This method must be called prior to generating the network.
     * 
     * @param distribution
     */
    public void setOutDistribution(Distribution distribution) {
        if (distribution==null) throw new NullPointerException();
        this.outDistribution=distribution;
    }

    /**
     * Sets the in-degree distribution to be used by this generator.
     * 
     * If the in-degree distribution is not specified, {@link CopyDistribution} is used by default.
     *
     * @param distribution
     */
    public void setInDistribution(Distribution distribution) {
        if (distribution==null) throw new NullPointerException();
        this.inDistribution=distribution;
    }

    /**
     * Clears the in-degree distribution
     */
    public void clearInDistribution() {
        this.inDistribution=null;
    }

    /**
     * Whether edge loops are allowed
     *
     * @return
     */
    public boolean hasAllowLoops() {
        return allowLoops;
    }

    /**
     * Sets whether loops, i.e. edges with the same start and end vertex, are allowed to be generated.
     * @param allowLoops
     */
    public void setAllowLoops(boolean allowLoops) {
        this.allowLoops=allowLoops;
    }

    /**
     * Generates a synthetic network connecting all vertices in the provided graph with the expected number
     * of edges.
     *
     * @param graph
     * @param expectedNumEdges
     * @return The number of generated edges. Not that this number may not be equal to the expected number of edges
     */
    public int generate(Graph graph, int expectedNumEdges) {
        return generate(graph,graph.getVertices(),expectedNumEdges);
    }

    /**
     * Generates a synthetic network connecting the given vertices by the expected number of directed edges
     * in the provided graph.
     *
     * @param graph
     * @param vertices
     * @param expectedNumEdges
     * @return The number of generated edges. Not that this number may not be equal to the expected number of edges
     */
    public int generate(Graph graph, Iterable<Vertex> vertices, int expectedNumEdges) {
        return generate(graph,vertices,vertices,expectedNumEdges);
    }

    /**
     * Generates a synthetic network connecting the vertices in <i>out</i> by directed edges
     * with those in <i>in</i> with the given number of expected edges in the provided graph.
     *
     * @param graph
     * @param out
     * @param in
     * @param expectedNumEdges
     * @return The number of generated edges. Not that this number may not be equal to the expected number of edges
     */
    public int generate(Graph graph, Iterable<Vertex> out, Iterable<Vertex> in, int expectedNumEdges) {
        if (outDistribution==null) throw new IllegalStateException("Must set out-distribution before generating edges");
        
        Distribution outDist = outDistribution.initialize(SizableIterable.sizeOf(out),expectedNumEdges);
        Distribution inDist = null;
        if (inDistribution==null) {
            if (out!=in) throw new IllegalArgumentException("Need to specify in-distribution");
            inDist = new CopyDistribution();
        } else {
            inDist = inDistribution.initialize(SizableIterable.sizeOf(in),expectedNumEdges);
        }

        long seed = System.currentTimeMillis()*177;
        Random outRandom = new Random(seed);
        ArrayList<Vertex> outStubs = new ArrayList<Vertex>(expectedNumEdges);
        for (Vertex v : out) {
            int degree = outDist.nextValue(outRandom);
            for (int i=0;i<degree;i++) {
                outStubs.add(v);
            }
        }
        
        Collections.shuffle(outStubs);
        
        outRandom = new Random(seed);
        Random inRandom = new Random(System.currentTimeMillis()*14421);
        int addedEdges = 0;
        int position = 0;
        for (Vertex v : in) {
            int degree = inDist.nextConditionalValue(inRandom, outDist.nextValue(outRandom));
            for (int i=0;i<degree;i++) {
                Vertex other = null;
                while (other==null) {
                    if (position>=outStubs.size()) return addedEdges; //No more edges to connect
                    other = outStubs.get(position);
                    position++;
                    if (!allowLoops && v.equals(other)) other=null;
                }
                //Connect edge
                addEdge(graph,other,v);
                addedEdges++;
            }
        }
        return addedEdges;
    }


}
