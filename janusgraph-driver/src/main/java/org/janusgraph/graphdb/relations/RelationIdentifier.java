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

import com.google.common.base.Preconditions;
import org.janusgraph.util.encoding.LongEncoding;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public final class RelationIdentifier implements Serializable {

    public static final String TOSTRING_DELIMITER = "-";

    private final long outVertexId;
    private final long typeId;
    private final long relationId;
    private final long inVertexId;

    private RelationIdentifier() {
        outVertexId = 0;
        typeId = 0;
        relationId = 0;
        inVertexId = 0;
    }

    public RelationIdentifier(final long outVertexId, final long typeId, final long relationId, final long inVertexId) {
        this.outVertexId = outVertexId;
        this.typeId = typeId;
        this.relationId = relationId;
        this.inVertexId = inVertexId;
    }

    public long getRelationId() {
        return relationId;
    }

    public long getTypeId() {
        return typeId;
    }

    public long getOutVertexId() {
        return outVertexId;
    }

    public long getInVertexId() {
        Preconditions.checkState(inVertexId != 0);
        return inVertexId;
    }

    public static RelationIdentifier get(long[] ids) {
        if (ids.length != 3 && ids.length != 4)
            throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        for (int i = 0; i < 3; i++) {
            if (ids[i] < 0)
                throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        }
        return new RelationIdentifier(ids[1], ids[2], ids[0], ids.length == 4 ? ids[3] : 0);
    }

    public static RelationIdentifier get(int[] ids) {
        if (ids.length != 3 && ids.length != 4)
            throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        for (int i = 0; i < 3; i++) {
            if (ids[i] < 0)
                throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        }
        return new RelationIdentifier(ids[1], ids[2], ids[0], ids.length == 4 ? ids[3] : 0);
    }

    public long[] getLongRepresentation() {
        long[] r = new long[3 + (inVertexId != 0 ? 1 : 0)];
        r[0] = relationId;
        r[1] = outVertexId;
        r[2] = typeId;
        if (inVertexId != 0) r[3] = inVertexId;
        return r;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(relationId);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (!getClass().isInstance(other)) return false;
        RelationIdentifier oth = (RelationIdentifier) other;
        return relationId == oth.relationId && typeId == oth.typeId;
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(LongEncoding.encode(relationId)).append(TOSTRING_DELIMITER).append(LongEncoding.encode(outVertexId))
                .append(TOSTRING_DELIMITER).append(LongEncoding.encode(typeId));
        if (inVertexId != 0) s.append(TOSTRING_DELIMITER).append(LongEncoding.encode(inVertexId));
        return s.toString();
    }

    public static RelationIdentifier parse(String id) {
        String[] elements = id.split(TOSTRING_DELIMITER);
        if (elements.length != 3 && elements.length != 4)
            throw new IllegalArgumentException("Not a valid relation identifier: " + id);
        try {
            return new RelationIdentifier(LongEncoding.decode(elements[1]),
                    LongEncoding.decode(elements[2]),
                    LongEncoding.decode(elements[0]),
                    elements.length == 4 ? LongEncoding.decode(elements[3]) : 0);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid id - each token expected to be a number", e);
        }
    }
}
