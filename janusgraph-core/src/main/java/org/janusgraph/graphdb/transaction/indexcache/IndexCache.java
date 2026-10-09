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

import java.util.Collection;

/**
 * The vertex properties which a transaction has added, of the property keys which its graph queries look up, by value,
 * the properties of new vertices apart from those of vertices which existed before the transaction. A key is indexed
 * from its first lookup on, starting from the properties which the transaction has added by then, so that a
 * transaction pays for the keys it looks up only, and for any other added property with a map lookup. Values are
 * compared with {@link Object#equals(Object)}, as {@link org.janusgraph.core.attribute.Cmp#EQUAL} compares every value
 * but an array.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IndexCache {

    /**
     * Indexes a property which the transaction added, if its key is indexed.
     */
    void add(JanusGraphVertexProperty property);

    /**
     * Forgets a property which the transaction added and then removed.
     */
    void remove(JanusGraphVertexProperty property);

    /**
     * How many properties of the key with the value the transaction added, to new vertices if {@code ofNewVertices},
     * or else to vertices which existed before the transaction.
     */
    int count(PropertyKey key, Object value, boolean ofNewVertices);

    /**
     * The properties of the key with the value which the transaction added, to new vertices if {@code ofNewVertices},
     * or else to vertices which existed before the transaction. An implementation for a transaction which several
     * threads may use returns a copy, the others a view which the transaction's next change may change.
     */
    Collection<JanusGraphVertexProperty> get(PropertyKey key, Object value, boolean ofNewVertices);

    /**
     * All properties of the key which the transaction added, to new vertices if {@code ofNewVertices}, or else to
     * vertices which existed before the transaction; a copy or a view, as {@link #get(PropertyKey, Object, boolean)}.
     */
    Iterable<JanusGraphVertexProperty> getAll(PropertyKey key, boolean ofNewVertices);

    /**
     * Closes the cache which allows the cache to release allocated memory.
     * Calling any of the other methods after closing a cache has undetermined behavior.
     */
    void close();

}
