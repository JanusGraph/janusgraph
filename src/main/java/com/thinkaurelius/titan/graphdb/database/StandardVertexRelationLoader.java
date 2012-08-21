package com.thinkaurelius.titan.graphdb.database;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.relations.factory.RelationLoader;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Direction;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class StandardVertexRelationLoader implements VertexRelationLoader {

    private final InternalTitanVertex vertex;
    private final InternalTitanTransaction tx;
    private final RelationLoader factory;
    
    private InternalRelation relation=null;

    public StandardVertexRelationLoader(final InternalTitanVertex vertex, final RelationLoader factory) {
        this.vertex=vertex;
        this.tx=vertex.getTransaction();
        this.factory=factory;
    }

    @Override
    public void loadProperty(long propertyid, TitanKey key, Object attribute) {
        relation = factory.createExistingProperty(propertyid, key, vertex, attribute);
    }

    @Override
    public void loadEdge(long edgeid, TitanLabel label, Direction dir, long otherVertexId) {
        InternalTitanVertex otherVertex = tx.getExistingVertex(otherVertexId);
        switch(dir) {
            case IN:
                relation = factory.createExistingRelationship(edgeid, label, otherVertex,vertex);
                break;
            case OUT:
                relation = factory.createExistingRelationship(edgeid, label, vertex,otherVertex);
                break;
            default: throw new IllegalArgumentException("Unexpected direction: " + dir);
        }
    }

    @Override
    public void addRelationProperty(TitanKey key, Object attribute) {
        Preconditions.checkNotNull(relation);
        Preconditions.checkNotNull(attribute);
        factory.createExistingProperty(key, relation, attribute);
    }

    @Override
    public void addRelationEdge(TitanLabel label, long vertexId) {
        Preconditions.checkNotNull(relation);
        Preconditions.checkNotNull(vertexId>0);
        InternalTitanVertex otherVertex=tx.getExistingVertex(vertexId);
        factory.createExistingRelationship(label, relation, otherVertex);
    }

    @Override
    public long getVertexId() {
        return vertex.getID();
    }
}
