package com.thinkaurelius.faunus.formats.sequence.faunus01;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.util.ElementHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexConverter {

    public static FaunusVertex buildFaunusVertex(final FaunusVertex01 vertex01) {
        final FaunusVertex vertex = new FaunusVertex(vertex01.getIdAsLong());
        ElementHelper.copyProperties(vertex01, vertex);
        for (final Edge temp : vertex01.getEdges(Direction.OUT)) {
            final FaunusEdge01 edge01 = (FaunusEdge01) temp;
            final FaunusEdge edge = new FaunusEdge(edge01.getIdAsLong(), edge01.getVertexId(Direction.OUT), edge01.getVertexId(Direction.IN), edge01.getLabel());
            ElementHelper.copyProperties(edge01, edge);
            vertex.addEdge(Direction.OUT, edge);
        }
        for (final Edge temp : vertex01.getEdges(Direction.IN)) {
            final FaunusEdge01 edge01 = (FaunusEdge01) temp;
            final FaunusEdge edge = new FaunusEdge(edge01.getIdAsLong(), edge01.getVertexId(Direction.OUT), edge01.getVertexId(Direction.IN), edge01.getLabel());
            ElementHelper.copyProperties(edge01, edge);
            vertex.addEdge(Direction.IN, edge);
        }
        return vertex;
    }

    public static FaunusVertex01 buildFaunusVertex01(final FaunusVertex vertex) {
        final FaunusVertex01 vertex01 = new FaunusVertex01(vertex.getIdAsLong());
        ElementHelper.copyProperties(vertex, vertex01);
        for (final Edge temp : vertex.getEdges(Direction.OUT)) {
            final FaunusEdge edge = (FaunusEdge) temp;
            final FaunusEdge01 edge01 = new FaunusEdge01(edge.getIdAsLong(), edge.getVertexId(Direction.OUT), edge.getVertexId(Direction.IN), edge.getLabel());
            ElementHelper.copyProperties(edge, edge01);
            vertex01.addEdge(Direction.OUT, edge01);
        }
        for (final Edge temp : vertex.getEdges(Direction.IN)) {
            final FaunusEdge edge = (FaunusEdge) temp;
            final FaunusEdge01 edge01 = new FaunusEdge01(edge.getIdAsLong(), edge.getVertexId(Direction.OUT), edge.getVertexId(Direction.IN), edge.getLabel());
            ElementHelper.copyProperties(edge, edge01);
            vertex01.addEdge(Direction.IN, edge01);
        }
        return vertex01;
    }
}
