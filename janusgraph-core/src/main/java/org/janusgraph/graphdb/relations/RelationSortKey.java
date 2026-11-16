// Copyright 2026 JanusGraph Authors
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

import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.types.TypeInspector;

import java.util.function.LongFunction;

/**
 * Encodes the sort-key values of an edge for {@link RelationIdentifier#getSortKey()}, and decodes them back.
 *
 * <p>An edge of a MULTI label with a sort key is stored under the column
 * {@code [type+dir][sortKey][adjacentVertex][relationId]}. Without the sort-key values an identifier can only be
 * resolved by slicing {@code [type+dir]}, which reads every edge with that label on the vertex. With them every
 * column component is known and the edge is read with a single column lookup.
 *
 * <p>The values are written with {@link DataOutput#writeObjectByteOrder(Object, Class)} in sort-key order. That is
 * the encoding the edgestore column uses, before the byte flip applied for {@code DESC} sort order.
 */
public final class RelationSortKey {

    /**
     * Largest encoded sort key an identifier carries. Edges with longer sort-key values keep identifiers without
     * one, so that identifiers stay small; they are resolved by scanning as before.
     */
    public static final int MAX_ENCODED_BYTES = 64;

    private RelationSortKey() {}

    /**
     * Whether edges of this type can carry a sort key in their identifier: MULTI edge labels with a non-empty sort
     * key. FORK labels are excluded because resolving their identifiers deliberately does not constrain the
     * relation id, and a sort-key constraint must not change which relations that lookup finds.
     */
    public static boolean supports(InternalRelationType type) {
        return RelationIdentifier.SORT_KEY_SUPPORTED
            && type.isEdgeLabel()
            && !type.multiplicity().isConstrained()
            && type.getSortKey().length > 0
            && type.getConsistencyModifier() != ConsistencyModifier.FORK;
    }

    /**
     * @return the encoded sort-key values of the relation, or {@code null} if its type does not support them, a
     *         sort-key value is missing, or the encoding exceeds {@link #MAX_ENCODED_BYTES}
     */
    public static String encode(InternalRelation relation, Serializer serializer) {
        if (!relation.isEdge()) return null;
        return encode((InternalRelationType) relation.getType(),
            keyId -> relation.getValueDirect(relation.tx().getExistingPropertyKey(keyId)), relation.tx(), serializer);
    }

    /**
     * @param valueByKeyId the edge's value of the property key with the given id, or {@code null} if it has none
     * @return the encoded sort-key values, or {@code null} under the same conditions as
     *         {@link #encode(InternalRelation, Serializer)}
     */
    public static String encode(InternalRelationType type, LongFunction<Object> valueByKeyId, TypeInspector types,
                                Serializer serializer) {
        if (!supports(type)) return null;
        final long[] sortKey = type.getSortKey();
        final DataOutput out = serializer.getDataOutput(9 * sortKey.length);
        for (final long keyId : sortKey) {
            final PropertyKey key = types.getExistingPropertyKey(keyId);
            final Object value = valueByKeyId.apply(keyId);
            if (value == null) return null;
            out.writeObjectByteOrder(value, key.dataType());
            if (out.getPosition() > MAX_ENCODED_BYTES) return null;
        }
        return RelationIdentifier.encodeSortKey(out.getStaticBuffer().as(StaticBuffer.ARRAY_FACTORY));
    }

    /**
     * Returns the identifier of the relation, carrying its sort-key values if it supports them.
     */
    public static RelationIdentifier withSortKey(RelationIdentifier relationId, InternalRelation relation,
                                                 Serializer serializer) {
        if (relationId.hasSortKey()) return relationId;
        final String sortKey = encode(relation, serializer);
        return sortKey == null ? relationId : relationId.withSortKey(sortKey);
    }

    /**
     * @return the sort-key values in sort-key order, or {@code null} if the identifier carries none or they do not
     *         decode cleanly against the type's sort key
     */
    public static Object[] decode(RelationIdentifier relationId, InternalRelationType type, TypeInspector types,
                                  Serializer serializer) {
        if (!relationId.hasSortKey() || !supports(type)) return null;
        final long[] sortKey = type.getSortKey();
        final Object[] values = new Object[sortKey.length];
        try {
            final ReadBuffer in = new StaticArrayBuffer(RelationIdentifier.decodeSortKey(relationId.getSortKey()))
                .asReadBuffer();
            for (int i = 0; i < sortKey.length; i++) {
                final PropertyKey key = types.getExistingPropertyKey(sortKey[i]);
                values[i] = serializer.readObjectByteOrder(in, key.dataType());
                if (values[i] == null) return null;
            }
            return in.hasRemaining() ? null : values;
        } catch (RuntimeException e) {
            return null;
        }
    }
}
