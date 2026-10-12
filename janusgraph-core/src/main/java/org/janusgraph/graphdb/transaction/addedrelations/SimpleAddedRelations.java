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

package org.janusgraph.graphdb.transaction.addedrelations;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.relations.StandardRelation;

import javax.annotation.Nonnull;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleAddedRelations implements AddedRelationsContainer {

    private int propertiesSize = 0;

    /**
     * Flag indicate if new properties should be groupped using property key.
     * This is done for query optimization (especially for new vertices)
     */
    private final Boolean groupPropertiesByKey;

    private final Map<String, AddedPropertiesValue> propertiesMap;

    private final ObjectSet<InternalRelation> propertiesContainer;

    private final ObjectSet<InternalRelation> edgesContainer;

    /**
     * A vertex may gain many edges in a transaction, and every query of its relations reads those it gained. Once its
     * container holds more than this many edges, it keeps them by label as well, so that a query of some labels reads
     * only the edges of those.
     */
    static final int MAX_EDGES_WITHOUT_LABEL_INDEX = 32;

    //The edges by the id of their label, kept by a container which groups its properties by key once it holds more
    //than MAX_EDGES_WITHOUT_LABEL_INDEX edges; null before
    private LongObjectHashMap<ObjectHashSet<InternalRelation>> edgesByLabel;

    private final Map<Long, ObjectSet<InternalRelation>> previousRelContainer;

    public SimpleAddedRelations(Boolean groupPropertiesByKey) {
        this.groupPropertiesByKey = groupPropertiesByKey;
        //Every transaction has a container, and so does every vertex which gains relations in it, usually a few, so the
        //containers start at their default sizes and grow with the relations added
        this.propertiesContainer = (groupPropertiesByKey) ? new ObjectHashSet<>(0) : new ObjectHashSet<>();
        this.propertiesMap = new HashMap<>();
        this.edgesContainer = new ObjectHashSet<>();
        this.previousRelContainer = new HashMap<>();
    }

    @Override
    public boolean add(InternalRelation relation) {
        if (relation.isEdge()) {
            if (edgesContainer.add(relation)) {
                if (edgesByLabel != null) {
                    edgesOfLabel(relation).add(relation);
                } else if (groupPropertiesByKey && edgesContainer.size() > MAX_EDGES_WITHOUT_LABEL_INDEX) {
                    edgesByLabel = new LongObjectHashMap<>();
                    for (ObjectCursor<InternalRelation> edge : edgesContainer) {
                        edgesOfLabel(edge.value).add(edge.value);
                    }
                }
            }
        } else if (relation.isProperty()) {
            if (groupPropertiesByKey) {
                addProperty(relation);
            } else {
                propertiesContainer.add(relation);
            }
        } else {
            throw new IllegalArgumentException("Unexpected relation type " + relation.getType().label());
        }

        addPreviousRelation(relation);

        return true;
    }

    private boolean addProperty(InternalRelation relation) {

        String key = relation.getType().name();
        Cardinality cardinality = ((PropertyKey) relation.getType()).cardinality();
        AddedPropertiesValue propertiesValue = propertiesMap.computeIfAbsent(key, k -> {

            if (Cardinality.SINGLE.equals(cardinality)) {
                return new AddedPropertiesSingleValue();
            } else if (Cardinality.SET.equals(cardinality)) {
                return new AddedPropertiesSetValue();
            } else {
                return new AddedPropertiesListValue();
            }
        });

        this.propertiesSize += propertiesValue.addValue(relation);
        return true;
    }

    @Override
    public boolean remove(InternalRelation relation) {
        if (relation.isEdge()) {
            edgesContainer.removeAll(relation);
            if (edgesByLabel != null) {
                ObjectHashSet<InternalRelation> edges = edgesByLabel.get(relation.getType().longId());
                if (edges != null) {
                    edges.removeAll(relation);
                }
            }
        } else if (relation.isProperty()) {
            if(groupPropertiesByKey) {
                removeProperty(relation);
            } else {
                propertiesContainer.removeAll(relation);
            }
        } else {
            throw new IllegalArgumentException("Unexpected relation type " + relation.getType().label());
        }

        removePreviousRelation(relation);
        return true;
    }

    private boolean removeProperty(InternalRelation relation) {
        String key = relation.getType().name();
        AddedPropertiesValue propertiesValue = propertiesMap.computeIfPresent(key, (k, v) -> {
            this.propertiesSize -= v.removeValue(relation);
            return v;
        });
        if (propertiesValue != null) {
            if (propertiesValue.isNull()) {
                propertiesMap.remove(key);
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Iterable<InternalRelation> getView(Predicate<InternalRelation> filter) {
        return Iterables.filter(this::iterator, filter);
    }

    /**
     * Reads only the edges of the given labels once the container keeps its edges by label, and otherwise all of its
     * edges, if any of the types is a label; and all of its properties if any of the types is a key. Properties are
     * grouped by the name of their key, which a transaction that renames the key changes under this one.
     */
    @Override
    public Iterable<InternalRelation> getView(Set<? extends RelationType> types, Predicate<InternalRelation> filter) {
        if (types == null || !groupPropertiesByKey) {
            return Iterables.filter(this::iterator, filter);
        }
        return Iterables.filter(() -> iterator(types), filter);
    }

    @Override
    public Iterable<InternalRelation> getViewOfProperties(Predicate<InternalRelation> filter) {
        return Iterables.filter(toIterable(() -> Iterators.concat(getAllProperties(), toIterator(propertiesContainer))), filter);
    }

    @Override
    public List<InternalRelation> getViewOfProperties(String... keys) {
        Iterator<InternalRelation> iterator =
            (keys.length == 0 ? getAllProperties() : getPropertiesForKeys(keys));

        List<InternalRelation> result = new ArrayList<>(this.propertiesSize);
        iterator.forEachRemaining(result::add);
        return result;
    }

    @Override
    public List<InternalRelation> getViewOfProperties(String key, Object value) {
        Iterator<InternalRelation> iterator = getPropertiesForKey(key, value);
        List<InternalRelation> result = new ArrayList<>(this.propertiesSize);
        iterator.forEachRemaining(result::add);
        return result;
    }

    @Override
    public Iterable<InternalRelation> getViewOfPreviousRelations(long id) {
        if (previousRelContainer.containsKey(id)) {
            return toIterable(previousRelContainer.get(id));
        } else {
            return Collections.EMPTY_LIST;
        }
    }

    @Override
    public boolean isEmpty() {
        return edgesContainer.isEmpty() && propertiesContainer.isEmpty() && propertiesSize == 0;
    }

    @Override
    public Collection<InternalRelation> getAllUnsafe() {
        return Collections.unmodifiableCollection(new AbstractCollection<InternalRelation>() {
            @Override
            @Nonnull
            public Iterator<InternalRelation> iterator() {
                return SimpleAddedRelations.this.iterator();
            }

            @Override
            public int size() {
                return (propertiesSize + propertiesContainer.size() + edgesContainer.size());
            }
        });
    }

    @Override
    public void clear() {
        propertiesMap.values().forEach(AddedPropertiesValue::clear);
        propertiesMap.clear();
        propertiesSize = 0;
        propertiesContainer.release();
        edgesContainer.release();
        edgesByLabel = null;
        previousRelContainer.clear();
    }

    private ObjectHashSet<InternalRelation> edgesOfLabel(InternalRelation edge) {
        long labelId = edge.getType().longId();
        ObjectHashSet<InternalRelation> edges = edgesByLabel.get(labelId);
        if (edges == null) {
            edges = new ObjectHashSet<>();
            edgesByLabel.put(labelId, edges);
        }
        return edges;
    }

    private void addPreviousRelation(InternalRelation relation) {
        if (relation instanceof StandardRelation) {
            long prevId = ((StandardRelation) relation).getPreviousID();
            if (prevId > 0) {
                if (!previousRelContainer.containsKey(prevId)) {
                    previousRelContainer.put(prevId, new ObjectHashSet<>(1));
                }
                previousRelContainer.get(prevId).add(relation);
            }
        }
    }

    private void removePreviousRelation(InternalRelation relation) {
        if (relation instanceof StandardRelation) {
            long prevId = ((StandardRelation) relation).getPreviousID();
            if (previousRelContainer.containsKey(prevId)) {
                previousRelContainer.get(prevId).removeAll(relation);
            }
        }
    }

    private Iterator<InternalRelation> getAllProperties() {
        return Iterators.concat(this.propertiesMap.values().stream()
            .map(AddedPropertiesValue::getView)
            .iterator());
    }

    private Iterator<InternalRelation> getPropertiesForKeys(String... keys) {
        return Iterators.concat(Stream.of(keys)
            .filter(propertiesMap::containsKey)
            .map(k -> this.propertiesMap.get(k).getView()).iterator());
    }

    private Iterator<InternalRelation> getPropertiesForKey(String key, Object value) {
        AddedPropertiesValue propValue = propertiesMap.get(key);
        if (propValue == null) {
            return Collections.emptyIterator();
        } else {
            return propValue.getView(value);
        }
    }

    private Iterable<InternalRelation> toIterable(ObjectSet<InternalRelation> relationObjectSet) {
        return toIterable(() -> toIterator(relationObjectSet));
    }

    private Iterable<InternalRelation> toIterable(Callable<Iterator<InternalRelation>> internalRelationIterator) {
        return new FluentIterable<InternalRelation>() {
            public Iterator<InternalRelation> iterator() {
                try {
                    return internalRelationIterator.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private Iterator<InternalRelation> iterator() {
        return Iterators.concat(getAllProperties(), toIterator(propertiesContainer), toIterator(edgesContainer));
    }

    //Only called on a container which groups its properties by key. The view may hold relations of other types, which
    //the caller turns away, so each of the containers it reads is read once whatever the number of types it is read for.
    private Iterator<InternalRelation> iterator(Set<? extends RelationType> types) {
        List<Iterator<InternalRelation>> relations = new ArrayList<>(Math.min(types.size(), 4));
        boolean allProperties = false;
        boolean allEdges = false;
        for (RelationType type : types) {
            if (type.isPropertyKey()) {
                if (!allProperties) {
                    relations.add(getAllProperties());
                    allProperties = true;
                }
            } else if (edgesByLabel != null) {
                ObjectHashSet<InternalRelation> edges = edgesByLabel.get(type.longId());
                if (edges != null) {
                    relations.add(toIterator(edges));
                }
            } else if (!allEdges) {
                //A few edges, which aren't kept by label
                relations.add(toIterator(edgesContainer));
                allEdges = true;
            }
        }
        return Iterators.concat(relations.iterator());
    }

    private Iterator<InternalRelation> toIterator(ObjectSet<InternalRelation> relationObjectSet) {
        return Iterators.transform(relationObjectSet.iterator(), e -> {
            assert e != null;
            return e.value;
        });
    }
}
