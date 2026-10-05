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

package org.janusgraph.graphdb.transaction.indexcache;

import com.google.common.collect.Iterables;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.transaction.addedrelations.AddedRelationsContainer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The {@link IndexCache} of a transaction which is bound to one thread.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SimpleIndexCache implements IndexCache {

    //The relations which the transaction added, from which a key's index starts
    private final AddedRelationsContainer addedRelations;

    private final Map<PropertyKey, KeyIndex> indexes = new HashMap<>();

    /**
     * @param addedRelations the relations which the transaction adds, from which the index of a key starts
     */
    public SimpleIndexCache(AddedRelationsContainer addedRelations) {
        this.addedRelations = addedRelations;
    }

    @Override
    public void add(JanusGraphVertexProperty property) {
        final KeyIndex index = indexes.isEmpty() ? null : indexes.get(property.propertyKey());
        if (index != null) {
            index.add(property);
        }
    }

    @Override
    public void remove(JanusGraphVertexProperty property) {
        final KeyIndex index = indexes.isEmpty() ? null : indexes.get(property.propertyKey());
        if (index != null) {
            index.remove(property);
        }
    }

    @Override
    public int count(PropertyKey key, Object value, boolean ofNewVertices) {
        return index(key).count(value, ofNewVertices);
    }

    @Override
    public Collection<JanusGraphVertexProperty> get(PropertyKey key, Object value, boolean ofNewVertices) {
        return index(key).get(value, ofNewVertices);
    }

    @Override
    public Iterable<JanusGraphVertexProperty> getAll(PropertyKey key, boolean ofNewVertices) {
        return index(key).getAll(ofNewVertices);
    }

    //The index of the key, which its first lookup builds from the properties of the key which the transaction added
    private KeyIndex index(PropertyKey key) {
        KeyIndex index = indexes.get(key);
        if (index == null) {
            indexed();
            index = new KeyIndex();
            for (InternalRelation relation : addedRelations.getViewOfProperties(r -> key.equals(r.getType()))) {
                index.add((JanusGraphVertexProperty) relation);
            }
            //Registered once whole: a build which fails leaves the key to the next lookup rather than half indexed
            indexes.put(key, index);
        }
        return index;
    }

    //Called when a key is indexed, before the key's index is built from the added relations
    protected void indexed() {
    }

    @Override
    public void close() {
        indexes.clear();
    }

    /**
     * The properties of one key which the transaction added, by value, those of new vertices apart from the others.
     * A value maps to its only property, or to the set of its properties when there are several, so that a key whose
     * values are unique, as the keys which a transaction looks its vertices up by are, costs one entry per property.
     */
    private static final class KeyIndex {

        private final Map<Object, Object> ofNewVertices = new HashMap<>();
        private final Map<Object, Object> ofLoadedVertices = new HashMap<>();

        void add(JanusGraphVertexProperty property) {
            put(property.element().isNew() ? ofNewVertices : ofLoadedVertices, property);
        }

        //The property is looked for in both, whatever its vertex is by the time it is removed
        void remove(JanusGraphVertexProperty property) {
            delete(ofNewVertices, property);
            delete(ofLoadedVertices, property);
        }

        int count(Object value, boolean ofNewVertices) {
            final Object held = (ofNewVertices ? this.ofNewVertices : ofLoadedVertices).get(value);
            return held == null ? 0 : held instanceof Set ? ((Set<?>) held).size() : 1;
        }

        Collection<JanusGraphVertexProperty> get(Object value, boolean ofNewVertices) {
            return properties((ofNewVertices ? this.ofNewVertices : ofLoadedVertices).get(value));
        }

        Iterable<JanusGraphVertexProperty> getAll(boolean ofNewVertices) {
            return Iterables.concat(Iterables.transform((ofNewVertices ? this.ofNewVertices : ofLoadedVertices).values(),
                KeyIndex::properties));
        }

        @SuppressWarnings("unchecked")
        private static void put(Map<Object, Object> byValue, JanusGraphVertexProperty property) {
            final Object value = property.value();
            final Object held = byValue.get(value);
            if (held == null) {
                byValue.put(value, property);
            } else if (held instanceof Set) {
                ((Set<JanusGraphVertexProperty>) held).add(property);
            } else if (!held.equals(property)) {
                final Set<JanusGraphVertexProperty> properties = new HashSet<>(4);
                properties.add((JanusGraphVertexProperty) held);
                properties.add(property);
                byValue.put(value, properties);
            }
        }

        @SuppressWarnings("unchecked")
        private static void delete(Map<Object, Object> byValue, JanusGraphVertexProperty property) {
            final Object value = property.value();
            final Object held = byValue.get(value);
            if (held instanceof Set) {
                final Set<JanusGraphVertexProperty> properties = (Set<JanusGraphVertexProperty>) held;
                if (properties.remove(property) && properties.isEmpty()) {
                    byValue.remove(value);
                }
            } else if (property.equals(held)) {
                byValue.remove(value);
            }
        }

        @SuppressWarnings("unchecked")
        private static Collection<JanusGraphVertexProperty> properties(Object held) {
            if (held == null) {
                return Collections.emptyList();
            }
            if (held instanceof Set) {
                return Collections.unmodifiableSet((Set<JanusGraphVertexProperty>) held);
            }
            return Collections.singletonList((JanusGraphVertexProperty) held);
        }
    }
}
