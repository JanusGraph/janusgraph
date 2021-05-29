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
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.janusgraph.graphdb.internal.InternalRelation;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import javax.annotation.Nonnull;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleAddedRelations implements AddedRelationsContainer {
    private static final int INITIAL_ADDED_SIZE = 10;
    private final ObjectSet<InternalRelation> container;

    public SimpleAddedRelations() {
        this.container = new ObjectHashSet<>(INITIAL_ADDED_SIZE);
    }

    @Override
    public boolean add(InternalRelation relation) {
        container.add(relation);
        return true;
    }

    @Override
    public boolean remove(InternalRelation relation) {
        container.removeAll(relation);
        return true;
    }

    @Override
    public Iterable<InternalRelation> getView(Predicate<InternalRelation> filter) {
        return Iterables.filter(this::iterator, filter);
    }

    @Override
    public boolean isEmpty() {
        return container.isEmpty();
    }

    @Override
    public Collection<InternalRelation> getAll() {
        return Collections.unmodifiableCollection(new AbstractCollection<InternalRelation>() {
            @Override
            @Nonnull
            public Iterator<InternalRelation> iterator() {
                return SimpleAddedRelations.this.iterator();
            }

            @Override
            public int size() {
                return container.size();
            }
        });
    }

    @Override
    public void clear() {
        container.release();
    }

    private Iterator<InternalRelation> iterator() {
        return Iterators.transform(container.iterator(), e -> {
            assert e != null;
            return e.value;
        });
    }
}
