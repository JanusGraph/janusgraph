package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.QueryException;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyList;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.AtomicQuery;
import com.thinkaurelius.titan.graphdb.query.SimpleAtomicQuery;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.util.interval.AtomicInterval;
import com.thinkaurelius.titan.util.interval.DoesNotExist;
import com.tinkerpop.blueprints.Direction;

import java.util.Map;

public class VertexUtil {


    public static Iterable<InternalRelation> filterByQuery(final AtomicQuery query, Iterable<InternalRelation> iter) {
        if (iter == AdjacencyList.Empty) return iter;

        if (query.queryHidden() && query.queryUnmodifiable() && query.queryProperties()
                && query.queryRelationships() && !query.hasConstraints() && query.getLimit() == Long.MAX_VALUE)
            return iter;
        if (!query.queryProperties() && !query.queryRelationships())
            throw new QueryException("Query excludes both: properties and relationships");


        return Iterables.filter(iter, new Predicate<InternalRelation>() {

            private int counter = 0;

            private Map<String, Integer> typeLookup = null;
            private int checkValue = -1;

            @Override
            public boolean apply(InternalRelation e) {
                if (query.getLimit() <= counter) return false;
                if (!query.queryProperties() && e.isProperty()) return false;
                if (!query.queryRelationships() && e.isEdge()) return false;
                if (!query.queryHidden() && e.isHidden()) return false;
                if (!query.queryUnmodifiable() && !e.isModifiable()) return false;
                if (query.hasConstraints()) {

                    Map<String, Object> constraints = query.getConstraints();

                    if (typeLookup == null) {
                        ImmutableMap.Builder<String, Integer> b = ImmutableMap.builder();
                        int count = 0;
                        for (Map.Entry<String, Object> entry : constraints.entrySet()) {
                            Object constraint = entry.getValue();
                            if (constraint != null && constraint != DoesNotExist.INSTANCE) {
                                b.put(entry.getKey(), count);
                                count++;
                            }
                        }
                        typeLookup = b.build();
                        checkValue = (1 << count) - 1;
                    }

                    int bitcode = 0;
                    for (TitanRelation ie : e.getRelations()) {
                        String typename = ie.getType().getName();
                        if (constraints.containsKey(typename)) {
                            Object o = constraints.get(typename);
                            if (ie.isEdge()) {
                                if (o == null) return false;
                                if (o.equals(((TitanEdge) ie).getVertex(Direction.IN)))
                                    bitcode = bitcode | (1 << typeLookup.get(typename));
                                else return false;
                            } else {
                                assert o != null;
                                assert ie.isProperty();
                                Object attribute = ((TitanProperty) ie).getValue();
                                assert attribute != null;
                                assert o instanceof AtomicInterval;
                                AtomicInterval iv = (AtomicInterval) o;
                                if (iv.inInterval(attribute)) bitcode = bitcode | (1 << typeLookup.get(typename));
                                else return false;
                            }
                        }
                    }
                    if (bitcode != checkValue) return false;
                }
                counter++;
                return true;
            }

        });

    }


    public static final Iterable<InternalRelation> filterLoopEdges(Iterable<InternalRelation> iter, final InternalVertex v) {
        if (iter == AdjacencyList.Empty) return iter;
        else return Iterables.filter(iter, new Predicate<InternalRelation>() {

            @Override
            public boolean apply(InternalRelation edge) {
                if (edge.isLoop()) return false;
                else return true;
            }
        }
        );

    }


    public static final Iterable<InternalRelation> getQuerySpecificIterable(AdjacencyList edges, AtomicQuery query) {
        if (query.hasEdgeTypeCondition()) {
            assert query.getTypeCondition() != null;
            return edges.getEdges(query.getTypeCondition());
        } else if (query.hasGroupCondition()) {
            return edges.getEdges(query.getGroupCondition());
        } else {
            return edges.getEdges();
        }
    }


}
