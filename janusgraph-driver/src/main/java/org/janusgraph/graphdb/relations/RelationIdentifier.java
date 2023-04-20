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

import org.apache.commons.lang3.StringUtils;
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

import static org.janusgraph.util.encoding.LongEncoding.STRING_ENCODING_MARKER;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public final class RelationIdentifier implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationIdentifier.class);
    public static final String JANUSGRAPH_RELATION_DELIMITER = "JANUSGRAPH_RELATION_DELIMITER";
    public static final String TOSTRING_DELIMITER;

    private final Object outVertexId;
    private final long typeId;
    private final long relationId;
    private final Object inVertexId;

    static {
        String reservedKeyword = System.getProperty(JANUSGRAPH_RELATION_DELIMITER);
        if (StringUtils.isEmpty(reservedKeyword)) {
            reservedKeyword = System.getenv(JANUSGRAPH_RELATION_DELIMITER);
        }
        if (StringUtils.isNotEmpty(reservedKeyword)) {
            if (!StringUtils.isAsciiPrintable(reservedKeyword)) {
                throw new IllegalStateException("JanusGraph relation delimiter must be ASCII printable: " + reservedKeyword);
            }
            if (reservedKeyword.length() > 1) {
                throw new IllegalStateException("JanusGraph relation delimiter must be single character: " + reservedKeyword);
            }
            TOSTRING_DELIMITER = reservedKeyword;
            LOGGER.info("Loaded {} from system property, relation delimiter is: {}",
                JANUSGRAPH_RELATION_DELIMITER, TOSTRING_DELIMITER);
        } else {
            TOSTRING_DELIMITER = "-";
            LOGGER.info("Use default relation delimiter: {}", TOSTRING_DELIMITER);
        }
    }

    private RelationIdentifier() {
        outVertexId = null;
        typeId = 0;
        relationId = 0;
        inVertexId = null;
    }

    public RelationIdentifier(final Object outVertexId, final long typeId, final long relationId, final Object inVertexId) {
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

    public Object getOutVertexId() {
        return outVertexId;
    }

    public Object getInVertexId() {
        return inVertexId;
    }

    public static RelationIdentifier get(Object[] ids) {
        if (ids.length != 3 && ids.length != 4)
            throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        for (int i = 0; i < 3; i++) {
            if (ids[i] instanceof Number && ((Number) ids[i]).longValue() < 0)
                throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        }
        return new RelationIdentifier(ids[1], ((Number) ids[2]).longValue(), ((Number) ids[0]).longValue(), ids.length == 4 ? ids[3] : 0);
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
        s.append(LongEncoding.encode(relationId)).append(TOSTRING_DELIMITER);
        if (outVertexId instanceof Number) {
            s.append(LongEncoding.encode(((Number) outVertexId).longValue()));
        } else {
            assert outVertexId instanceof String;
            s.append(STRING_ENCODING_MARKER).append(outVertexId);
        }
        s.append(TOSTRING_DELIMITER).append(LongEncoding.encode(typeId));
        if (inVertexId != null) {
            if (inVertexId instanceof Number) {
                assert ((Number) inVertexId).longValue() > 0;
                s.append(TOSTRING_DELIMITER).append(LongEncoding.encode(((Number) inVertexId).longValue()));
            } else {
                assert inVertexId instanceof String;
                s.append(TOSTRING_DELIMITER).append(STRING_ENCODING_MARKER).append(inVertexId);
            }
        }
        return s.toString();
    }

    public static RelationIdentifier parse(String id) {
        String[] elements = id.split(TOSTRING_DELIMITER);
        if (elements.length != 3 && elements.length != 4)
            throw new IllegalArgumentException("Not a valid relation identifier: " + id);
        try {
            Object outVertexId;
            if (elements[1].charAt(0) == STRING_ENCODING_MARKER) {
                outVertexId = elements[1].substring(1);
            } else {
                outVertexId = LongEncoding.decode(elements[1]);
            }
            final long typeId = LongEncoding.decode(elements[2]);
            final long relationId = LongEncoding.decode(elements[0]);
            Object inVertexId = null;
            if (elements.length == 4) {
                if (elements[3].charAt(0) == STRING_ENCODING_MARKER) {
                    inVertexId = elements[3].substring(1);
                } else {
                    inVertexId = LongEncoding.decode(elements[3]);
                }
            }
            return new RelationIdentifier(outVertexId, typeId, relationId, inVertexId);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid id - each token expected to be a number", e);
        }
    }
}
