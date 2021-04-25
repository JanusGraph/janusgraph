// Copyright 2019 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.relations;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.query.vertex.VertexCentricQueryBuilder;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.system.ImplicitKey;

public class RelationIdentifierUtils {
    public static RelationIdentifier get(InternalRelation r) {
        if (r.hasId()) {
            return new RelationIdentifier(r.getVertex(0).longId(),
                r.getType().longId(),
                r.longId(), (r.isEdge() ? r.getVertex(1).longId() : 0));
        } else return null;
    }

    protected static JanusGraphRelation findRelation(RelationIdentifier rId, JanusGraphTransaction tx) {
        JanusGraphVertex v = ((StandardJanusGraphTx)tx).getInternalVertex(rId.getOutVertexId());
        if (v == null || v.isRemoved()) return null;

        JanusGraphVertex typeVertex = tx.getVertex(rId.getTypeId());
        if (typeVertex == null) return null;

        if (!(typeVertex instanceof RelationType))
            throw new IllegalArgumentException("Invalid RelationIdentifier: typeID does not reference a type");

        Iterable<? extends JanusGraphRelation> relations = getJanusGraphRelations(rId, tx, v, (RelationType) typeVertex);

        for (JanusGraphRelation r : relations) {
            //Find current or previous relation
            if (r.longId() == rId.getRelationId() ||
                ((r instanceof StandardRelation) && ((StandardRelation) r).getPreviousID() == rId.getRelationId())) return r;
        }
        return null;
    }

    private static Iterable<? extends JanusGraphRelation> getJanusGraphRelations(RelationIdentifier rId, JanusGraphTransaction tx, JanusGraphVertex v, RelationType typeVertex) {
        if (typeVertex.isEdgeLabel()) {
            return findEdgeRelations(v, typeVertex, rId, tx);
        } else {
            return ((VertexCentricQueryBuilder) v.query()).noPartitionRestriction().types(typeVertex).properties();
        }
    }

    public static Iterable<? extends JanusGraphRelation> findEdgeRelations(JanusGraphVertex v, RelationType type, RelationIdentifier rId, JanusGraphTransaction tx){
        Direction dir = Direction.OUT;
        JanusGraphVertex other = ((StandardJanusGraphTx)tx).getInternalVertex(rId.getInVertexId());
        if (other==null || other.isRemoved()) return null;
        if (((StandardJanusGraphTx) tx).isPartitionedVertex(v) && !((StandardJanusGraphTx) tx).isPartitionedVertex(other)) { //Swap for likely better performance
            JanusGraphVertex tmp = other;
            other = v;
            v = tmp;
            dir = Direction.IN;
        }
        VertexCentricQueryBuilder query =
            ((VertexCentricQueryBuilder) v.query()).noPartitionRestriction().types(type).direction(dir).adjacent(other);

        RelationType internalVertex = ((StandardJanusGraphTx) tx).getExistingRelationType(type.longId());
        if (((InternalRelationType) internalVertex).getConsistencyModifier() != ConsistencyModifier.FORK) {
            query.has(ImplicitKey.JANUSGRAPHID.name(), rId.getRelationId());
        }
        return query.edges();
    }

    public static JanusGraphEdge findEdge(RelationIdentifier rId, JanusGraphTransaction tx) {
        JanusGraphRelation r = findRelation(rId, tx);
        if (r == null) return null;
        else if (r instanceof JanusGraphEdge) return (JanusGraphEdge) r;
        else throw new UnsupportedOperationException("Referenced relation is a property not an edge");
    }

    public static JanusGraphVertexProperty findProperty(RelationIdentifier rId, JanusGraphTransaction tx) {
        JanusGraphRelation r = findRelation(rId, tx);
        if (r == null) return null;
        else if (r instanceof JanusGraphVertexProperty) return (JanusGraphVertexProperty) r;
        else throw new UnsupportedOperationException("Referenced relation is a edge not a property");
    }
}
