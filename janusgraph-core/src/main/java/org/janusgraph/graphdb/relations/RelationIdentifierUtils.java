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
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.query.vertex.VertexCentricQueryBuilder;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.ImplicitKey;

import java.util.Collections;

public class RelationIdentifierUtils {
    public static RelationIdentifier get(InternalRelation r, long relationId) {
        if (relationId > 0) {
            RelationIdentifier rId = new RelationIdentifier(r.getVertex(0).id(),
                r.getType().longId(),
                relationId, (r.isEdge() ? r.getVertex(1).id() : null));
            return rId;
        } else return null;
    }

    protected static JanusGraphRelation findRelation(RelationIdentifier rId, JanusGraphTransaction tx) {
        JanusGraphVertex v = ((StandardJanusGraphTx)tx).getInternalVertex(rId.getOutVertexId());
        if (v == null || v.isRemoved()) return null;

        StandardJanusGraphTx stx = (StandardJanusGraphTx) tx;
        long typeId = rId.getTypeId();
        RelationType typeVertex;
        if (typeId <= 0 || !stx.getIdInspector().isRelationTypeId(typeId)
            || (typeId >>> IDManager.MAX_PADDING_BITWIDTH) >= IDManager.getSchemaCountBound()) {
            // Malformed identifier (not a relation type id, or one outside the schema id range): not on the hot path,
            // keep the original resolution so behaviour is unchanged (no edge if nothing exists under that id,
            // IllegalArgumentException if a non-type vertex does).
            JanusGraphVertex vertex = tx.getVertex(typeId);
            if (vertex == null) return null;
            if (!(vertex instanceof RelationType))
                throw new IllegalArgumentException("Invalid RelationIdentifier: typeID does not reference a type");
            typeVertex = (RelationType) vertex;
        } else if (IDManager.isSystemRelationTypeId(typeId) || stx.isVertexCached(typeId)) {
            // System types, and types created or already loaded in this transaction, need no storage access.
            // A cached handle may also be the removed stub left behind by an earlier existence check that found nothing.
            typeVertex = stx.getExistingRelationType(typeId);
            if (typeVertex == null || typeVertex.isRemoved()) return null; // unknown system type id, or known to be missing
        } else if (stx.getGraph().getSchemaCache().getSchemaRelations(typeId, BaseKey.SchemaDefinitionProperty, Direction.OUT).isEmpty()) {
            // No such type in storage (stale or foreign identifier). Resolved through the schema cache instead of
            // tx.getVertex(), which would issue a vertex-existence read against the schema row in every transaction.
            // The schema cache never remembers an empty definition, so a type that is not visible yet is re-read on
            // the next lookup, and nothing is added to this transaction's vertex cache for it.
            return null;
        } else {
            typeVertex = stx.getExistingRelationType(typeId);
        }

        Iterable<? extends JanusGraphRelation> relations = getJanusGraphRelations(rId, tx, v, typeVertex);

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
        if (other==null || other.isRemoved()) {
            // The adjacent (in-)vertex no longer exists, so the edge is gone with it: there are no relations to find.
            // Empty (never null) so callers can iterate the result unconditionally.
            return Collections.emptyList();
        }
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
