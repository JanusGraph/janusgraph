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

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectSet;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
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
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleAddedRelations implements AddedRelationsContainer {
    private static final int INITIAL_PROP_ADDED_SIZE = 30;

    private static final int INITIAL_EDGE_ADDED_SIZE = 300;

    private int propertiesSize = 0;

    /**
     * Flag indicate if new properties should be groupped using property key.
     * This is done for query optimization (especially for new vertices)
     */
    private final Boolean groupPropertiesByKey;

    private final Map<String, AddedPropertiesValue> propertiesMap;

    private final ObjectSet<InternalRelation> propertiesContainer;

    private final ObjectSet<InternalRelation> edgesContainer;

    private final Map<Long, ObjectSet<InternalRelation>> previousRelContainer;

    public SimpleAddedRelations(Boolean groupPropertiesByKey) {
        this.groupPropertiesByKey = groupPropertiesByKey;
        this.propertiesContainer = new ObjectHashSet<>((groupPropertiesByKey) ? 0 : INITIAL_PROP_ADDED_SIZE);
        this.propertiesMap = new HashMap<>((groupPropertiesByKey) ? INITIAL_PROP_ADDED_SIZE : 0);
        this.edgesContainer = new ObjectHashSet<>(INITIAL_EDGE_ADDED_SIZE);
        this.previousRelContainer = new HashMap<>(INITIAL_EDGE_ADDED_SIZE + INITIAL_PROP_ADDED_SIZE);
    }

    @Override
    public boolean add(InternalRelation relation) {
        if (relation.isEdge()) {
            edgesContainer.add(relation);
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
        previousRelContainer.clear();
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

    private Iterator<InternalRelation> toIterator(ObjectSet<InternalRelation> relationObjectSet) {
        return Iterators.transform(relationObjectSet.iterator(), e -> {
            assert e != null;
            return e.value;
        });
    }
}
