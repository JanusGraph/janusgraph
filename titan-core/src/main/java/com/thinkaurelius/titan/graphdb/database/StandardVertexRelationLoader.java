package com.thinkaurelius.titan.graphdb.database;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.adjacencylist.StandardAdjListFactory;
import com.thinkaurelius.titan.graphdb.relations.InlineProperty;
import com.thinkaurelius.titan.graphdb.relations.InlineTitanEdge;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.relations.factory.RelationFactoryUtil;
import com.thinkaurelius.titan.graphdb.relations.persist.PersistLabeledTitanEdge;
import com.thinkaurelius.titan.graphdb.relations.persist.PersistSimpleProperty;
import com.thinkaurelius.titan.graphdb.relations.persist.PersistSimpleTitanEdge;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Direction;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class StandardVertexRelationLoader implements VertexRelationLoader {

    private final InternalTitanVertex vertex;
    private final InternalTitanTransaction tx;

    private InternalRelation relation = null;

    public StandardVertexRelationLoader(final InternalTitanVertex vertex) {
        this.vertex = vertex;
        this.tx = vertex.getTransaction();
    }

    @Override
    public void loadProperty(long propertyid, TitanKey key, Object attribute) {
        Preconditions.checkArgument(relation == null, "Need to finalize previous relation");
        Preconditions.checkNotNull(attribute);
        if (key.isSimple()) {
            relation = new PersistSimpleProperty(key, vertex, attribute, propertyid);
        } else throw new UnsupportedOperationException();
    }

    @Override
    public void loadEdge(long edgeid, TitanLabel label, Direction dir, long otherVertexId) {
        Preconditions.checkArgument(relation == null, "Need to finalize previous relation");
        InternalTitanVertex otherVertex = tx.getExistingVertex(otherVertexId);
        InternalTitanVertex start, end;
        switch (dir) {
            case IN:
                start = otherVertex;
                end = vertex;
                break;
            case OUT:
                start = vertex;
                end = otherVertex;
                break;
            default:
                throw new IllegalArgumentException("Unexpected direction: " + dir);
        }
        if (label.isSimple()) {
            relation = new PersistSimpleTitanEdge(label, start, end, edgeid);
        } else {
            relation = new PersistLabeledTitanEdge(label, start, end, tx, StandardAdjListFactory.INSTANCE, edgeid);
        }
    }

    @Override
    public void finalizeRelation() {
        Preconditions.checkNotNull(relation, "No relation in progress");
        RelationFactoryUtil.connectRelation(relation, false, tx);
        relation = null;
    }


    @Override
    public void addRelationProperty(TitanKey key, Object attribute) {
        Preconditions.checkNotNull(relation, "No relation in progress");
        Preconditions.checkNotNull(attribute);
        Preconditions.checkArgument(key.isFunctional(), key.getName());
        Preconditions.checkArgument(!relation.getType().isSimple());
        InternalRelation rel = new InlineProperty(key, relation, attribute);
        RelationFactoryUtil.connectRelation(rel, false, tx);
    }

    @Override
    public void addRelationEdge(TitanLabel label, long vertexId) {
        Preconditions.checkNotNull(relation, "No relation in progress");
        Preconditions.checkNotNull(vertexId > 0);
        Preconditions.checkArgument(!relation.getType().isSimple());
        Preconditions.checkArgument(label.isUnidirected() && label.isSimple());

        InternalTitanVertex otherVertex = tx.getExistingVertex(vertexId);
        InternalRelation inline = new InlineTitanEdge(label, relation, otherVertex);
        RelationFactoryUtil.connectRelation(inline, false, tx);
    }

    @Override
    public long getVertexId() {
        return vertex.getID();
    }

}
