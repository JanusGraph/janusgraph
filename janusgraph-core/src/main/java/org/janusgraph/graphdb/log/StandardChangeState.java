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

    private final EnumMap<Change,Set<JanusVertex>> vertices;
    private final EnumMap<Change,Set<JanusRelation>> relations;


    StandardChangeState() {
        vertices = new EnumMap<Change, Set<JanusVertex>>(Change.class);
        relations = new EnumMap<Change, Set<JanusRelation>>(Change.class);
        for (Change state : new Change[]{Change.ADDED,Change.REMOVED}) {
            vertices.put(state,new HashSet<JanusVertex>());
            relations.put(state,new HashSet<JanusRelation>());
        }
    }


    void addVertex(InternalVertex vertex, Change state) {
        vertices.get(state).add(vertex);
    }

    void addRelation(InternalRelation rel, Change state) {
        relations.get(state).add(rel);
    }

    @Override
    public Set<JanusVertex> getVertices(Change change) {
        if (change.isProper()) return vertices.get(change);
        assert change==Change.ANY;
        Set<JanusVertex> all = new HashSet<JanusVertex>();
        for (Change state : new Change[]{Change.ADDED,Change.REMOVED}) {
            all.addAll(vertices.get(state));
            for (JanusRelation rel : relations.get(state)) {
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

    private Iterable<JanusRelation> getRelations(final Change change, final Predicate<JanusRelation> filter) {
        Iterable<JanusRelation> base;
        if(change.isProper()) base=relations.get(change);
        else base=Iterables.concat(relations.get(Change.ADDED),relations.get(Change.REMOVED));
        return Iterables.filter(base,filter);
    }

    @Override
    public Iterable<JanusRelation> getRelations(final Change change, final RelationType... types) {
        final Set<RelationType> stypes = toSet(types);
        return getRelations(change, new Predicate<JanusRelation>() {
            @Override
            public boolean apply(@Nullable JanusRelation janusRelation) {
                return stypes.isEmpty() || stypes.contains(janusRelation.getType());
            }
        });
    }

    @Override
    public Iterable<JanusEdge> getEdges(final Vertex vertex, final Change change, final Direction dir, final String... labels) {
        final Set<String> stypes = toSet(labels);
        return (Iterable)getRelations(change, new Predicate<JanusRelation>() {
            @Override
            public boolean apply(@Nullable JanusRelation janusRelation) {
                return janusRelation.isEdge() && janusRelation.isIncidentOn(vertex) &&
                        (dir==Direction.BOTH || ((JanusEdge)janusRelation).vertex(dir).equals(vertex)) &&
                        (stypes.isEmpty() || stypes.contains(janusRelation.getType().name()));
            }
        });
    }


    @Override
    public Iterable<JanusVertexProperty> getProperties(final Vertex vertex, final Change change, final String... keys) {
        final Set<String> stypes = toSet(keys);
        return (Iterable)getRelations(change, new Predicate<JanusRelation>() {
            @Override
            public boolean apply(@Nullable JanusRelation janusRelation) {
                return janusRelation.isProperty() && janusRelation.isIncidentOn(vertex) &&
                        (stypes.isEmpty() || stypes.contains(janusRelation.getType().name()));
            }
        });
    }

}
