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

import com.google.common.io.BaseEncoding;
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

// Encodes the sort-key values of an edge as a hex string, and decodes them back.
//
// An edge of a MULTI label with a sort key is stored under the column [type+dir][sortKey][adjacentVertex][relationId].
// Its identifier does not carry the sort-key values, so resolving it slices [type+dir] and reads every edge of the
// label on the vertex. A mixed index can store the encoded values next to the edge's document (see
// IndexProvider.EDGE_SORT_KEY_FIELD) and return them with each hit, so the hit is resolved with a single column read.
//
// The values are written with DataOutput.writeObjectByteOrder in sort-key order: the encoding the edgestore column
// uses, before the byte flip applied for DESC sort order.
public final class RelationSortKey {

    // Largest encoded sort key that is stored. Edges with longer sort-key values are indexed without one and are
    // resolved by scanning as before.
    public static final int MAX_ENCODED_BYTES = 256;

    private static final BaseEncoding HEX = BaseEncoding.base16().lowerCase();

    private RelationSortKey() {}

    // Whether edges of this type can be resolved by their sort-key values: MULTI edge labels with a non-empty sort
    // key. FORK labels are excluded because resolving them deliberately does not constrain the relation id, and a
    // sort-key constraint must not change which relations that lookup finds.
    public static boolean supports(InternalRelationType type) {
        return type.isEdgeLabel()
            && !type.multiplicity().isConstrained()
            && type.getSortKey().length > 0
            && type.getConsistencyModifier() != ConsistencyModifier.FORK;
    }

    // The encoded sort-key values of the relation, or null if its type does not support them, a sort-key value is
    // missing, or the encoding exceeds MAX_ENCODED_BYTES.
    public static String encode(InternalRelation relation, Serializer serializer) {
        if (!relation.isEdge()) return null;
        final InternalRelationType type = (InternalRelationType) relation.getType();
        if (!supports(type)) return null;
        final long[] sortKey = type.getSortKey();
        final DataOutput out = serializer.getDataOutput(9 * sortKey.length);
        for (final long keyId : sortKey) {
            final PropertyKey key = relation.tx().getExistingPropertyKey(keyId);
            final Object value = relation.getValueDirect(key);
            if (value == null) return null;
            out.writeObjectByteOrder(value, key.dataType());
            if (out.getPosition() > MAX_ENCODED_BYTES) return null;
        }
        return HEX.encode(out.getStaticBuffer().as(StaticBuffer.ARRAY_FACTORY));
    }

    // The sort-key values in sort-key order, or null if they are absent or do not decode cleanly against the type's
    // sort key.
    public static Object[] decode(String encoded, InternalRelationType type, TypeInspector types,
                                  Serializer serializer) {
        if (encoded == null || encoded.isEmpty() || !supports(type)) return null;
        final long[] sortKey = type.getSortKey();
        final Object[] values = new Object[sortKey.length];
        try {
            final byte[] bytes = HEX.decode(encoded);
            if (bytes.length > MAX_ENCODED_BYTES) return null;
            final ReadBuffer in = new StaticArrayBuffer(bytes).asReadBuffer();
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
