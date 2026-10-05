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

import com.google.common.collect.Lists;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.transaction.addedrelations.AddedRelationsContainer;

import java.util.ArrayList;
import java.util.Collection;

/**
 * The {@link IndexCache} of a transaction which isn't bound to a thread, as {@code graph.newTransaction()} and
 * {@code createThreadedTx()} start, and which several threads may use. Lookups return copies.
 * <p>
 * A key's index is built under this cache's monitor. The transaction adds a relation to its added relations before it
 * adds the relation here, and removes it from there before it removes it here, so a property which another thread adds
 * or removes meanwhile ends up indexed or not as the transaction holds it: a property which the build finds among the
 * added relations and which then reaches this cache is indexed once, as indexing is idempotent. Until a key has been
 * indexed, adding and removing properties takes no lock: the flag which tells is set before the first build reads the
 * added relations.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ConcurrentIndexCache extends SimpleIndexCache {

    private volatile boolean anyIndexed;

    /**
     * @param addedRelations the relations which the transaction adds, from which the index of a key starts
     */
    public ConcurrentIndexCache(AddedRelationsContainer addedRelations) {
        super(addedRelations);
    }

    @Override
    public void add(JanusGraphVertexProperty property) {
        if (anyIndexed) {
            synchronized (this) {
                super.add(property);
            }
        }
    }

    @Override
    public void remove(JanusGraphVertexProperty property) {
        if (anyIndexed) {
            synchronized (this) {
                super.remove(property);
            }
        }
    }

    @Override
    protected void indexed() {
        anyIndexed = true;
    }

    @Override
    public synchronized int count(PropertyKey key, Object value, boolean ofNewVertices) {
        return super.count(key, value, ofNewVertices);
    }

    @Override
    public synchronized Collection<JanusGraphVertexProperty> get(PropertyKey key, Object value, boolean ofNewVertices) {
        return new ArrayList<>(super.get(key, value, ofNewVertices));
    }

    @Override
    public synchronized Iterable<JanusGraphVertexProperty> getAll(PropertyKey key, boolean ofNewVertices) {
        return Lists.newArrayList(super.getAll(key, ofNewVertices));
    }

    @Override
    public synchronized void close() {
        super.close();
    }
}
