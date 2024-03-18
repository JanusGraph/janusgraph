// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.core;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.system.BaseRelationType;
import org.janusgraph.graphdb.types.system.SystemRelationType;

public class JanusGraphLazyRelationConstructor {

    public static InternalRelation create(InternalRelation janusGraphRelation,
                                          final InternalVertex vertex,
                                          final StandardJanusGraphTx tx) {
        long typeId = janusGraphRelation.getType().longId();
        InternalRelationType type = tx.getOrLoadRelationTypeById(typeId);
        JanusGraphLazyRelation lazyRelation;
        if (type.isPropertyKey()) {
            lazyRelation = new JanusGraphLazyVertexProperty(janusGraphRelation, vertex, tx, type);
        } else {
            lazyRelation = new JanusGraphLazyEdge(janusGraphRelation, vertex, tx, type);
        }

        if (type instanceof BaseRelationType) {
            return lazyRelation.loadValue();
        } else {
            return lazyRelation;
        }
    }

    public static InternalRelation create(Entry dataEntry,
                                          final InternalVertex vertex,
                                          final StandardJanusGraphTx tx) {

        long typeId = tx.getEdgeSerializer().parseTypeId(dataEntry);
        InternalRelationType type = tx.getOrLoadRelationTypeById(typeId);

        JanusGraphLazyRelation lazyRelation;
        if (type.isPropertyKey()) {
            lazyRelation = new JanusGraphLazyVertexProperty(dataEntry, vertex, tx, type);
        } else {
            lazyRelation = new JanusGraphLazyEdge(dataEntry, vertex, tx, type);
        }

        if (type instanceof SystemRelationType) {
            return lazyRelation.loadValue();
        } else {
            return lazyRelation;
        }
    }
}
