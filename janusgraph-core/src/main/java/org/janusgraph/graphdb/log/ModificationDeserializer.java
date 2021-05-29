// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.graphdb.log;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.log.Change;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.graphdb.database.log.TransactionLogHeader;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.relations.CacheEdge;
import org.janusgraph.graphdb.relations.CacheVertexProperty;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.relations.StandardEdge;
import org.janusgraph.graphdb.relations.StandardVertexProperty;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ModificationDeserializer {


    public static InternalRelation parseRelation(TransactionLogHeader.Modification modification, StandardJanusGraphTx tx) {
        Change state = modification.state;
        assert state.isProper();
        long outVertexId = modification.outVertexId;
        Entry relEntry = modification.relationEntry;
        InternalVertex outVertex = tx.getInternalVertex(outVertexId);
        //Special relation parsing, compare to {@link RelationConstructor}
        RelationCache relCache = tx.getEdgeSerializer().readRelation(relEntry, false, tx);
        assert relCache.direction == Direction.OUT;
        InternalRelationType type = (InternalRelationType)tx.getExistingRelationType(relCache.typeId);
        assert type.getBaseType()==null;
        InternalRelation rel;
        if (type.isPropertyKey()) {
            if (state==Change.REMOVED) {
                rel = new StandardVertexProperty(relCache.relationId,(PropertyKey)type,outVertex,relCache.getValue(), ElementLifeCycle.Removed);
            } else {
                rel = new CacheVertexProperty(relCache.relationId,(PropertyKey)type,outVertex,relCache.getValue(),relEntry);
            }
        } else {
            assert type.isEdgeLabel();
            InternalVertex otherVertex = tx.getInternalVertex(relCache.getOtherVertexId());
            if (state==Change.REMOVED) {
                rel = new StandardEdge(relCache.relationId, (EdgeLabel) type, outVertex, otherVertex,ElementLifeCycle.Removed);
            } else {
                rel = new CacheEdge(relCache.relationId, (EdgeLabel) type, outVertex, otherVertex,relEntry);
            }
        }
        if (state==Change.REMOVED && relCache.hasProperties()) { //copy over properties
            for (LongObjectCursor<Object> entry : relCache) {
                rel.setPropertyDirect(tx.getExistingPropertyKey(entry.key),entry.value);
            }
        }
        return rel;
    }

}
