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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.janusgraph.core.*;
import org.janusgraph.core.log.Change;
import org.janusgraph.core.log.ChangeState;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
class StandardChangeState implements ChangeState {

    private final EnumMap<Change,Set<JanusGraphVertex>> vertices;
    private final EnumMap<Change,Set<JanusGraphRelation>> relations;


    StandardChangeState() {
        vertices = new EnumMap<Change, Set<JanusGraphVertex>>(Change.class);
        relations = new EnumMap<Change, Set<JanusGraphRelation>>(Change.class);
        for (Change state : new Change[]{Change.ADDED,Change.REMOVED}) {
            vertices.put(state,new HashSet<JanusGraphVertex>());
            relations.put(state,new HashSet<JanusGraphRelation>());
        }
    }


    void addVertex(InternalVertex vertex, Change state) {
        vertices.get(state).add(vertex);
    }

    void addRelation(InternalRelation rel, Change state) {
        relations.get(state).add(rel);
    }

    @Override
    public Set<JanusGraphVertex> getVertices(Change change) {
        if (change.isProper()) return vertices.get(change);
        assert change==Change.ANY;
        Set<JanusGraphVertex> all = new HashSet<JanusGraphVertex>();
        for (Change state : new Change[]{Change.ADDED,Change.REMOVED}) {
            all.addAll(vertices.get(state));
            for (JanusGraphRelation rel : relations.get(state)) {
                InternalRelation irel = (InternalRelation)rel;
                for (int p=0;p<irel.getLen();p++) all.add(irel.getVertex(p));
            }
        }
        return all;
    }

    private<T> Set<T> toSet(T... types) {
        if (types==null || types.length==0) return Sets.newHashSet();
        return Sets.newHashSet(types);
    }

    private Iterable<JanusGraphRelation> getRelations(final Change change, final Predicate<JanusGraphRelation> filter) {
        Iterable<JanusGraphRelation> base;
        if(change.isProper()) base=relations.get(change);
        else base=Iterables.concat(relations.get(Change.ADDED),relations.get(Change.REMOVED));
        return Iterables.filter(base,filter);
    }

    @Override
    public Iterable<JanusGraphRelation> getRelations(final Change change, final RelationType... types) {
        final Set<RelationType> stypes = toSet(types);
        return getRelations(change, new Predicate<JanusGraphRelation>() {
            @Override
            public boolean apply(@Nullable JanusGraphRelation janusgraphRelation) {
                return stypes.isEmpty() || stypes.contains(janusgraphRelation.getType());
            }
        });
    }

    @Override
    public Iterable<JanusGraphEdge> getEdges(final Vertex vertex, final Change change, final Direction dir, final String... labels) {
        final Set<String> stypes = toSet(labels);
        return (Iterable)getRelations(change, new Predicate<JanusGraphRelation>() {
            @Override
            public boolean apply(@Nullable JanusGraphRelation janusgraphRelation) {
                return janusgraphRelation.isEdge() && janusgraphRelation.isIncidentOn(vertex) &&
                        (dir==Direction.BOTH || ((JanusGraphEdge)janusgraphRelation).vertex(dir).equals(vertex)) &&
                        (stypes.isEmpty() || stypes.contains(janusgraphRelation.getType().name()));
            }
        });
    }


    @Override
    public Iterable<JanusGraphVertexProperty> getProperties(final Vertex vertex, final Change change, final String... keys) {
        final Set<String> stypes = toSet(keys);
        return (Iterable)getRelations(change, new Predicate<JanusGraphRelation>() {
            @Override
            public boolean apply(@Nullable JanusGraphRelation janusgraphRelation) {
                return janusgraphRelation.isProperty() && janusgraphRelation.isIncidentOn(vertex) &&
                        (stypes.isEmpty() || stypes.contains(janusgraphRelation.getType().name()));
            }
        });
    }

}
