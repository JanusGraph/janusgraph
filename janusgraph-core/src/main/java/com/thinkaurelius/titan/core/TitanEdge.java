
package com.thinkaurelius.titan.core;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.List;

/**
 * A TitanEdge connects two {@link TitanVertex}. It extends the functionality provided by Blueprint's {@link Edge} and
 * is a special case of a {@link TitanRelation}.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see Edge
 * @see TitanRelation
 * @see EdgeLabel
 */
public interface TitanEdge extends TitanRelation, Edge {

    /**
     * Returns the edge label of this edge
     *
     * @return edge label of this edge
     */
    public default EdgeLabel edgeLabel() {
        assert getType() instanceof EdgeLabel;
        return (EdgeLabel)getType();
    }

    /**
     * Returns the vertex for the specified direction.
     * The direction cannot be Direction.BOTH.
     *
     * @return the vertex for the specified direction
     */
    public TitanVertex vertex(Direction dir);

    @Override
    public default TitanVertex outVertex() {
        return vertex(Direction.OUT);
    }

    @Override
    public default TitanVertex inVertex() {
        return vertex(Direction.IN);
    }


    /**
     * Returns the vertex at the opposite end of the edge.
     *
     * @param vertex vertex on which this edge is incident
     * @return The vertex at the opposite end of the edge.
     * @throws InvalidElementException if the edge is not incident on the specified vertex
     */
    public TitanVertex otherVertex(Vertex vertex);


    @Override
    public default Iterator<Vertex> vertices(Direction direction) {
        List<Vertex> vertices;
        if (direction==Direction.BOTH) {
            vertices = ImmutableList.of((Vertex) vertex(Direction.OUT), vertex(Direction.IN));
        } else {
            vertices = ImmutableList.of((Vertex) vertex(direction));
        }
        return vertices.iterator();
    }

}
