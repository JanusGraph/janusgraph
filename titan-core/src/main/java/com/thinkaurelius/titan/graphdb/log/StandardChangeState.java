package com.thinkaurelius.titan.graphdb.log;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.log.Change;
import com.thinkaurelius.titan.core.log.ChangeState;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
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

    private final EnumMap<Change,Set<TitanVertex>> vertices;
    private final EnumMap<Change,Set<TitanRelation>> relations;


    StandardChangeState() {
        vertices = new EnumMap<Change, Set<TitanVertex>>(Change.class);
        relations = new EnumMap<Change, Set<TitanRelation>>(Change.class);
        for (Change state : new Change[]{Change.ADDED,Change.REMOVED}) {
            vertices.put(state,new HashSet<TitanVertex>());
            relations.put(state,new HashSet<TitanRelation>());
        }
    }


    void addVertex(InternalVertex vertex, Change state) {
        vertices.get(state).add(vertex);
    }

    void addRelation(InternalRelation rel, Change state) {
        relations.get(state).add(rel);
    }

    @Override
    public Set<TitanVertex> getVertices(Change change) {
        if (change.isProper()) return vertices.get(change);
        assert change==Change.ANY;
        Set<TitanVertex> all = new HashSet<TitanVertex>();
        for (Change state : new Change[]{Change.ADDED,Change.REMOVED}) {
            all.addAll(vertices.get(state));
            for (TitanRelation rel : relations.get(state)) {
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

    private Iterable<TitanRelation> getRelations(final Change change, final Predicate<TitanRelation> filter) {
        Iterable<TitanRelation> base;
        if(change.isProper()) base=relations.get(change);
        else base=Iterables.concat(relations.get(Change.ADDED),relations.get(Change.REMOVED));
        return Iterables.filter(base,filter);
    }

    @Override
    public Iterable<TitanRelation> getRelations(final Change change, final RelationType... types) {
        final Set<RelationType> stypes = toSet(types);
        return getRelations(change, new Predicate<TitanRelation>() {
            @Override
            public boolean apply(@Nullable TitanRelation titanRelation) {
                return stypes.isEmpty() || stypes.contains(titanRelation.getType());
            }
        });
    }

    @Override
    public Iterable<TitanEdge> getEdges(final Vertex vertex, final Change change, final Direction dir, final String... labels) {
        final Set<String> stypes = toSet(labels);
        return (Iterable)getRelations(change, new Predicate<TitanRelation>() {
            @Override
            public boolean apply(@Nullable TitanRelation titanRelation) {
                return titanRelation.isEdge() && titanRelation.isIncidentOn(vertex) &&
                        (dir==Direction.BOTH || ((TitanEdge)titanRelation).vertex(dir).equals(vertex)) &&
                        (stypes.isEmpty() || stypes.contains(titanRelation.getType().name()));
            }
        });
    }


    @Override
    public Iterable<TitanVertexProperty> getProperties(final Vertex vertex, final Change change, final String... keys) {
        final Set<String> stypes = toSet(keys);
        return (Iterable)getRelations(change, new Predicate<TitanRelation>() {
            @Override
            public boolean apply(@Nullable TitanRelation titanRelation) {
                return titanRelation.isProperty() && titanRelation.isIncidentOn(vertex) &&
                        (stypes.isEmpty() || stypes.contains(titanRelation.getType().name()));
            }
        });
    }

}
