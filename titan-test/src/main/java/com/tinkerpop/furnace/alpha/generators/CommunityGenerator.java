package com.tinkerpop.furnace.alpha.generators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

/**
 * Generates a synthetic network with a community structure, that is, several densely connected
 * sub-networks that are loosely connected with one another.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CommunityGenerator extends AbstractGenerator {

    /**
     * Default value used if {@link #setCrossCommunityPercentage(double)} is not called.
     */
    public static final double DEFAULT_CROSS_COMMUNITY_PERCENTAGE = 0.1;

    private Distribution communitySize=null;
    private Distribution edgeDegree=null;
    private double crossCommunityPercentage = DEFAULT_CROSS_COMMUNITY_PERCENTAGE;
    
    private final Random random = new Random();

    /**
     *
     * @param label
     * @param annotator
     * @see AbstractGenerator#AbstractGenerator(String, EdgeAnnotator)
     */
    public CommunityGenerator(String label, EdgeAnnotator annotator) {
        super(label, annotator);
    }

    /**
     *
     * @param label
     * @param edgeAnnotator
     * @param vertexAnnotator
     * @see AbstractGenerator#AbstractGenerator(String, EdgeAnnotator, VertexAnnotator)
     */
    public CommunityGenerator(String label, EdgeAnnotator edgeAnnotator, VertexAnnotator vertexAnnotator) {
        super(label, edgeAnnotator, vertexAnnotator);
    }

    /**
     *
     * @param label
     * @see AbstractGenerator#AbstractGenerator(String)
     */
    public CommunityGenerator(String label) {
        super(label);
    }

    /**
     * Sets the distribution to be used to generate the sizes of communities.
     *
     * @param community
     */
    public void setCommunityDistribution(Distribution community) {
        this.communitySize=community;
    }

    /**
     * Sets the distribution to be used to generate the out-degrees of vertices
     *
     * @param degree
     */
    public void setDegreeDistribution(Distribution degree) {
        this.edgeDegree=degree;
    }

    /**
     * Sets the percentage of edges that cross a community, i.e. connect a vertex to a vertex in
     * another community. The lower this value, the higher the modularity of the generated communities.
     *
     * @param percentage Percentage of community crossing edges. Must be in [0,1]
     */
    public void setCrossCommunityPercentage(double percentage) {
        if (percentage<0.0 || percentage>1.0) throw new IllegalArgumentException("Percentage must be between 0 and 1");
        this.crossCommunityPercentage=percentage;
    }

    /**
     * Returns the configured cross community percentage.
     *
     * @return
     */
    public double getCrossCommunityPercentage() {
        return crossCommunityPercentage;
    }

    /**
     * Generates a synthetic network for all vertices in the given graph such that the provided expected number
     * of communities are generated with the specified expected number of edges.
     *
     * @param graph
     * @param expectedNumCommunities
     * @param expectedNumEdges
     * @return The actual number of edges generated. May be different from the expected number.
     */
    public int generate(Graph graph, int expectedNumCommunities, int expectedNumEdges) {
        return generate(graph,graph.getVertices(),expectedNumCommunities,expectedNumEdges);
    }


    /**
     * Generates a synthetic network for provided vertices in the given graphh such that the provided expected number
     * of communities are generated with the specified expected number of edges.
     *
     * @param graph
     * @param vertices
     * @param expectedNumCommunities
     * @param expectedNumEdges
     * @return The actual number of edges generated. May be different from the expected number.
     */
    public int generate(Graph graph, Iterable<Vertex> vertices, int expectedNumCommunities, int expectedNumEdges) {
        if (communitySize==null) throw new IllegalStateException("Need to initialize community size distribution");
        if (edgeDegree==null) throw new IllegalStateException("Need to initialize degree distribution");
        int numVertices = SizableIterable.sizeOf(vertices);
        Iterator<Vertex> iter = vertices.iterator();
        ArrayList<ArrayList<Vertex>> communities = new ArrayList<ArrayList<Vertex>>(expectedNumCommunities);
        Distribution communityDist = communitySize.initialize(expectedNumCommunities,numVertices);
        Map<String, Object> context = new HashMap<String, Object>();
        while (iter.hasNext()) {
            int nextSize = communityDist.nextValue(random);
            context.put("communityIndex", communities.size());
            ArrayList<Vertex> community = new ArrayList<Vertex>(nextSize);
            for (int i=0;i<nextSize && iter.hasNext();i++) {
                community.add(processVertex(iter.next(), context));
            }
            if (!community.isEmpty()) communities.add(community);
        }

        double inCommunityPercentage = 1.0-crossCommunityPercentage;
        Distribution degreeDist = edgeDegree.initialize(numVertices,expectedNumEdges);
        if (crossCommunityPercentage>0 && communities.size()<2) throw new IllegalArgumentException("Cannot have cross links with only one community");
        int addedEdges = 0;
        
        //System.out.println("Generating links on communities: "+communities.size());

        for (ArrayList<Vertex> community : communities) {
            for (Vertex v : community) {
                int degree = degreeDist.nextValue(random);
                degree = Math.min(degree,(int)Math.ceil((community.size() - 1) / inCommunityPercentage)-1);
                Set<Vertex> inlinks = new HashSet<Vertex>();
                Set<Vertex> outlinks = new HashSet<Vertex>();
                for (int i=0;i<degree;i++) {
                    Vertex selected = null;
                    if (random.nextDouble()<crossCommunityPercentage || (community.size()-1<=inlinks.size()) ) {
                        //Cross community
                        ArrayList<Vertex> othercomm = null;
                        while (selected == null) {
                            while (othercomm==null) {
                                othercomm = communities.get(random.nextInt(communities.size()));
                                if (othercomm.equals(community)) othercomm=null;
                            }
                            selected = othercomm.get(random.nextInt(othercomm.size()));
                            if (outlinks.contains(selected)) selected = null;
                        }
                        outlinks.add(selected);
                    } else {
                        //In community
                        while (selected==null) {
                            selected=community.get(random.nextInt(community.size()));
                            if (v.equals(selected) || inlinks.contains(selected)) selected=null;
                        }
                        inlinks.add(selected);
                    }
                    addEdge(graph,v,selected);
                    addedEdges++;
                }
            }
        }
        return addedEdges;
    }
    
}
