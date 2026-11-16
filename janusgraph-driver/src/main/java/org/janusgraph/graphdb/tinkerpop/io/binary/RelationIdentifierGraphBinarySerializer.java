// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.io.binary;

import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.janusgraph.graphdb.relations.RelationIdentifier;

import java.io.IOException;

/**
 * Serialize and deserialize relation identifier
 * To distinguish between string and long IDs, we use an extra byte as marker.
 * Technically, we could save this byte by leveraging the fact that long-type IDs
 * are always non-negative, but it would require changes from TinkerPop side as
 * well.
 */
public class RelationIdentifierGraphBinarySerializer extends JanusGraphTypeSerializer<RelationIdentifier> {

    public static final byte LONG_MARKER = 0;
    public static final byte STRING_MARKER = 1;
    /**
     * In-vertex markers of an edge identifier that carries a sort key: the in-vertex id as for
     * {@link #LONG_MARKER} / {@link #STRING_MARKER}, followed by the sort-key length (int) and bytes.
     * Only written when a sort key is present, so identifiers without one keep the original format.
     */
    public static final byte LONG_WITH_SORT_KEY_MARKER = 2;
    public static final byte STRING_WITH_SORT_KEY_MARKER = 3;

    public RelationIdentifierGraphBinarySerializer() {
        super(GraphBinaryType.RelationIdentifier);
    }

    private void writeString(Buffer buffer, String value) {
        byte[] arr = new byte[value.length()];
        for (int i = 0; i < value.length(); i++) {
            int c = value.charAt(i);
            assert c <= 127;
            byte b = (byte)c;
            if (i+1==value.length()) b |= 0x80; //End marker
            arr[i] = b;
        }
        buffer.writeBytes(arr);
    }

    private String readString(Buffer buffer) {
        StringBuilder sb = new StringBuilder();
        while (true) {
            int c = 0xFF & buffer.readByte();
            sb.append((char)(c & 0x7F));
            if ((c & 0x80) > 0) break;
        }
        return sb.toString();
    }

    @Override
    public RelationIdentifier readNonNullableValue(Buffer buffer, GraphBinaryReader context) throws IOException {
        final byte outVertexIdMarker = buffer.readByte();
        Object outVertexId;
        if (outVertexIdMarker == STRING_MARKER) {
            outVertexId = readString(buffer);
        } else {
            outVertexId = buffer.readLong();
        }
        final long typeId = buffer.readLong();
        final long relationId = buffer.readLong();
        final byte inVertexIdMarker = buffer.readByte();
        Object inVertexId;
        if (inVertexIdMarker == STRING_MARKER || inVertexIdMarker == STRING_WITH_SORT_KEY_MARKER) {
            inVertexId = readString(buffer);
        } else {
            inVertexId = buffer.readLong();
            if (inVertexId.equals(0L)) {
                inVertexId = null;
            }
        }
        final RelationIdentifier relationIdentifier = new RelationIdentifier(outVertexId, typeId, relationId, inVertexId);
        if (inVertexIdMarker == LONG_WITH_SORT_KEY_MARKER || inVertexIdMarker == STRING_WITH_SORT_KEY_MARKER) {
            final byte[] sortKey = new byte[buffer.readInt()];
            buffer.readBytes(sortKey);
            return relationIdentifier.withSortKey(RelationIdentifier.encodeSortKey(sortKey));
        }
        return relationIdentifier;
    }

    @Override
    protected void writeNonNullableValue(RelationIdentifier value, Buffer buffer, GraphBinaryWriter context) throws IOException {
        Object outVertexId = value.getOutVertexId();
        if (outVertexId instanceof Number) {
            buffer.writeByte(LONG_MARKER);
            buffer.writeLong(((Number) outVertexId).longValue());
        } else {
            assert outVertexId instanceof String;
            buffer.writeByte(STRING_MARKER);
            writeString(buffer, (String) outVertexId);
        }
        buffer.writeLong(value.getTypeId());
        buffer.writeLong(value.getRelationId());
        Object inVertexId = value.getInVertexId();
        if (inVertexId == null) {
            // properties don't have in-vertex
            buffer.writeByte(LONG_MARKER);
            buffer.writeLong(0);
        } else if (inVertexId instanceof Number) {
            buffer.writeByte(value.hasSortKey() ? LONG_WITH_SORT_KEY_MARKER : LONG_MARKER);
            buffer.writeLong(((Number) inVertexId).longValue());
        } else {
            assert inVertexId instanceof String;
            buffer.writeByte(value.hasSortKey() ? STRING_WITH_SORT_KEY_MARKER : STRING_MARKER);
            writeString(buffer, (String) inVertexId);
        }
        if (inVertexId != null && value.hasSortKey()) {
            final byte[] sortKey = RelationIdentifier.decodeSortKey(value.getSortKey());
            buffer.writeInt(sortKey.length);
            buffer.writeBytes(sortKey);
        }
    }
}
