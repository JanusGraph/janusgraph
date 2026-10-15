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

import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.transaction.addedrelations.AddedRelationsContainer;

import java.util.ArrayList;
import java.util.Arrays;
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
        return index(key).of(ofNewVertices).count(value);
    }

    @Override
    public Collection<JanusGraphVertexProperty> get(PropertyKey key, Object value, boolean ofNewVertices) {
        return index(key).of(ofNewVertices).get(value);
    }

    @Override
    public Iterable<JanusGraphVertexProperty> getAll(PropertyKey key, boolean ofNewVertices) {
        return index(key).of(ofNewVertices).getAll();
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
     * The properties of one key which the transaction added, those of new vertices apart from the others.
     */
    private static final class KeyIndex {

        private final Properties ofNewVertices = new Properties();
        private final Properties ofLoadedVertices = new Properties();

        void add(JanusGraphVertexProperty property) {
            of(property.element().isNew()).add(property);
        }

        //The property is looked for in both, whatever its vertex is by the time it is removed
        void remove(JanusGraphVertexProperty property) {
            ofNewVertices.remove(property);
            ofLoadedVertices.remove(property);
        }

        Properties of(boolean ofNewVertices) {
            return ofNewVertices ? this.ofNewVertices : ofLoadedVertices;
        }
    }

    /**
     * Properties of one key which the transaction added, by value. A value maps to its only property, or to the set of
     * its properties when there are several, so that a key whose values are unique, as the keys which a transaction
     * looks its vertices up by are, costs one entry per property. Once all of them are asked for, they are kept in an
     * array as well, in which a later property goes after the last one, or into a larger copy, and in which no element
     * changes once written: all of them are then returned as they are without a copy, as long as none has been removed
     * since, as a removal drops the array, which the next request builds again.
     */
    private static final class Properties {

        private final Map<Object, Object> byValue = new HashMap<>();
        //The properties in byValue, in the first size elements, or null until they are asked for or since a removal
        private JanusGraphVertexProperty[] all;
        private int size;

        void add(JanusGraphVertexProperty property) {
            if (put(property) && all != null) {
                append(property);
            }
        }

        void remove(JanusGraphVertexProperty property) {
            if (delete(property)) {
                all = null;
            }
        }

        int count(Object value) {
            final Object held = byValue.get(value);
            return held == null ? 0 : held instanceof Set ? ((Set<?>) held).size() : 1;
        }

        //The properties of one value, a set of several of them copied, which the transaction's later changes then don't
        //change
        @SuppressWarnings("unchecked")
        Collection<JanusGraphVertexProperty> get(Object value) {
            final Object held = byValue.get(value);
            if (held == null) {
                return Collections.emptyList();
            }
            if (held instanceof Set) {
                return new ArrayList<>((Set<JanusGraphVertexProperty>) held);
            }
            return Collections.singletonList((JanusGraphVertexProperty) held);
        }

        //The first elements of the array, which a later property, going after them, and a removal, dropping the
        //array, leave as they are
        @SuppressWarnings("unchecked")
        Collection<JanusGraphVertexProperty> getAll() {
            if (byValue.isEmpty()) {
                return Collections.emptyList();
            }
            if (all == null) {
                all = new JanusGraphVertexProperty[Math.max(8, byValue.size() + (byValue.size() >> 1))];
                size = 0;
                for (final Object held : byValue.values()) {
                    if (held instanceof Set) {
                        for (final JanusGraphVertexProperty property : (Set<JanusGraphVertexProperty>) held) {
                            append(property);
                        }
                    } else {
                        append((JanusGraphVertexProperty) held);
                    }
                }
            }
            return Collections.unmodifiableList(Arrays.asList(all).subList(0, size));
        }

        private void append(JanusGraphVertexProperty property) {
            if (size == all.length) {
                all = Arrays.copyOf(all, 2 * size);
            }
            all[size++] = property;
        }

        //Whether the property wasn't held yet
        @SuppressWarnings("unchecked")
        private boolean put(JanusGraphVertexProperty property) {
            final Object value = property.value();
            final Object held = byValue.get(value);
            if (held == null) {
                byValue.put(value, property);
                return true;
            }
            if (held instanceof Set) {
                return ((Set<JanusGraphVertexProperty>) held).add(property);
            }
            if (held.equals(property)) {
                return false;
            }
            final Set<JanusGraphVertexProperty> properties = new HashSet<>(4);
            properties.add((JanusGraphVertexProperty) held);
            properties.add(property);
            byValue.put(value, properties);
            return true;
        }

        //Whether the property was held
        @SuppressWarnings("unchecked")
        private boolean delete(JanusGraphVertexProperty property) {
            final Object value = property.value();
            final Object held = byValue.get(value);
            if (held instanceof Set) {
                final Set<JanusGraphVertexProperty> properties = (Set<JanusGraphVertexProperty>) held;
                if (!properties.remove(property)) {
                    return false;
                }
                if (properties.isEmpty()) {
                    byValue.remove(value);
                }
                return true;
            }
            if (property.equals(held)) {
                byValue.remove(value);
                return true;
            }
            return false;
        }
    }
}
