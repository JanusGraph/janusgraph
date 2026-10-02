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

package org.janusgraph.graphdb.relations;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collections;
import java.util.Iterator;

/**
 * Immutable map from long key ids to objects.
 * Implemented for memory and time efficiency.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RelationCache implements Iterable<LongObjectCursor<Object>> {

    private static final LongObjectHashMap<Object> EMPTY = new LongObjectHashMap<>(0);

    public final Direction direction;
    public final long typeId;
    public final long relationId;
    private final Object other;
    private final LongObjectHashMap<Object> properties;

    public RelationCache(final Direction direction, final long typeId, final long relationId,
                         final Object other, final LongObjectHashMap<Object> properties) {
        this.direction = direction;
        this.typeId = typeId;
        this.relationId = relationId;
        this.other = other;
        this.properties = (properties == null || properties.size() > 0) ? properties : EMPTY;
    }

    /**
     * A relation parsed header-only: its type, direction, id and other end are known, its properties are not.
     */
    public RelationCache(final Direction direction, final long typeId, final long relationId,
                         final Object other) {
        this(direction,typeId,relationId,other,null);
    }

    /**
     * A relation parsed with its properties, of which it has none: {@link #isFullyParsed()} and not
     * {@link #hasProperties()}.
     */
    public static RelationCache withoutProperties(final Direction direction, final long typeId, final long relationId,
                                                  final Object other) {
        return new RelationCache(direction, typeId, relationId, other, EMPTY);
    }

    @SuppressWarnings("unchecked")
    public <O> O get(long key) {
        return (O) properties.get(key);
    }

    /**
     * Whether the properties of the relation were parsed. Only a fully parsed cache answers {@link #get(long)},
     * {@link #numProperties()} and iteration; a cache parsed header-only has to be parsed again for them. A fully
     * parsed relation without properties is fully parsed all the same, which is what tells it apart from a
     * header-only one.
     */
    public boolean isFullyParsed() {
        return properties != null;
    }

    public boolean hasProperties() {
        return properties != null && !properties.isEmpty();
    }

    public int numProperties() {
        return properties.size();
    }

    public Object getValue() {
        return other;
    }

    public Object getOtherVertexId() {
        return other;
    }

    public Iterator<LongObjectCursor<Object>> propertyIterator() {
        //Iterating an HPPC map writes its iteration seed, and the empty map is shared by every relation without
        //properties, so it isn't iterated
        return properties.isEmpty() ? Collections.emptyIterator() : properties.iterator();
    }

    @Override
    public Iterator<LongObjectCursor<Object>> iterator() {
        return propertyIterator();
    }

    @Override
    public String toString() {
         return typeId + "-" + direction + "->" + other + ":" + relationId;
    }

}
