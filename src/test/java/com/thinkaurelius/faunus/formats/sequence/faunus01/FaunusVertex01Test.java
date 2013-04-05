package com.thinkaurelius.faunus.formats.sequence.faunus01;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusVertex01Test extends BaseTest {

    public void testConversionOneStep() throws Exception {
        for (final FaunusVertex a : BaseTest.generateGraph(ExampleGraph.GRAPH_OF_THE_GODS).values()) {
            for (final FaunusVertex b : BaseTest.generateGraph(ExampleGraph.GRAPH_OF_THE_GODS).values()) {
                if (a.getIdAsLong() == b.getIdAsLong()) {
                    assertTrue(haveEqualNeighborhood(a, b, true));
                    assertTrue(haveEqualNeighborhood(a, b, false));
                    assertTrue(haveEqualNeighborhood(a, VertexConverter.buildFaunusVertex01(b), true));
                    assertTrue(haveEqualNeighborhood(a, VertexConverter.buildFaunusVertex01(b), false));
                } else {
                    assertFalse(haveEqualNeighborhood(a, b, true));
                    assertFalse(haveEqualNeighborhood(a, b, false));
                    assertFalse(haveEqualNeighborhood(a, VertexConverter.buildFaunusVertex01(b), true));
                    assertFalse(haveEqualNeighborhood(a, VertexConverter.buildFaunusVertex01(b), false));
                }
            }
        }
    }

    public void testConversionTwoStep() throws Exception {
        for (final FaunusVertex a : BaseTest.generateGraph(ExampleGraph.GRAPH_OF_THE_GODS).values()) {
            for (final FaunusVertex b : BaseTest.generateGraph(ExampleGraph.GRAPH_OF_THE_GODS).values()) {
                if (a.getIdAsLong() == b.getIdAsLong()) {
                    assertTrue(haveEqualNeighborhood(a, b, true));
                    assertTrue(haveEqualNeighborhood(a, b, false));
                    assertTrue(haveEqualNeighborhood(a, VertexConverter.buildFaunusVertex(VertexConverter.buildFaunusVertex01(b)), true));
                    assertTrue(haveEqualNeighborhood(a, VertexConverter.buildFaunusVertex(VertexConverter.buildFaunusVertex01(b)), false));
                } else {
                    assertFalse(haveEqualNeighborhood(a, b, true));
                    assertFalse(haveEqualNeighborhood(a, b, false));
                    assertFalse(haveEqualNeighborhood(a, VertexConverter.buildFaunusVertex(VertexConverter.buildFaunusVertex01(b)), true));
                    assertFalse(haveEqualNeighborhood(a, VertexConverter.buildFaunusVertex(VertexConverter.buildFaunusVertex01(b)), false));
                }
            }
        }
    }

    // TODO: REMOVE WHEN BLUEPRINTS 2.3.2 IS RELEASED

    public static boolean haveEqualNeighborhood(final Vertex a, final Vertex b, final boolean checkIdEquality) {
        if (checkIdEquality && !a.getId().equals(b.getId()))
            return false;

        return ElementHelper.haveEqualProperties(a, b) && haveEqualEdges(a, b, checkIdEquality);
    }


    public static boolean haveEqualEdges(final Vertex a, final Vertex b, boolean checkIdEquality) {
        Set<Edge> aEdgeSet = new HashSet<Edge>();
        for (Edge edge : a.getEdges(Direction.OUT)) {
            aEdgeSet.add(edge);
        }
        Set<Edge> bEdgeSet = new HashSet<Edge>();
        for (Edge edge : b.getEdges(Direction.OUT)) {
            bEdgeSet.add(edge);
        }
        if (!hasEqualEdgeSets(aEdgeSet, bEdgeSet, checkIdEquality))
            return false;

        aEdgeSet.clear();
        bEdgeSet.clear();

        for (Edge edge : a.getEdges(Direction.IN)) {
            aEdgeSet.add(edge);
        }
        for (Edge edge : b.getEdges(Direction.IN)) {
            bEdgeSet.add(edge);
        }
        return hasEqualEdgeSets(aEdgeSet, bEdgeSet, checkIdEquality);

    }

    private static boolean hasEqualEdgeSets(final Set<Edge> aEdgeSet, final Set<Edge> bEdgeSet, final boolean checkIdEquality) {
        if (aEdgeSet.size() != bEdgeSet.size())
            return false;

        for (Edge aEdge : aEdgeSet) {
            Edge tempEdge = null;
            for (Edge bEdge : bEdgeSet) {
                if (bEdge.getLabel().equals(aEdge.getLabel())) {
                    if (checkIdEquality) {
                        if (aEdge.getId().equals(bEdge.getId()) &&
                                aEdge.getVertex(Direction.IN).getId().equals(bEdge.getVertex(Direction.IN).getId()) &&
                                aEdge.getVertex(Direction.OUT).getId().equals(bEdge.getVertex(Direction.OUT).getId()) &&
                                ElementHelper.haveEqualProperties(aEdge, bEdge)) {
                            tempEdge = bEdge;
                            break;
                        }
                    } else if (ElementHelper.haveEqualProperties(aEdge, bEdge)) {
                        tempEdge = bEdge;
                        break;
                    }
                }
            }
            if (tempEdge == null)
                return false;
            else
                bEdgeSet.remove(tempEdge);
        }
        return bEdgeSet.size() == 0;
    }
}
