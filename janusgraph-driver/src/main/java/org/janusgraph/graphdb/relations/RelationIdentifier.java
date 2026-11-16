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
 * Identifies a relation (edge or property) and how to find it in the storage backend.
 *
 * <p>An edge whose label is MULTI and has a sort key is stored under a column that contains its sort-key values
 * ahead of the adjacent vertex and relation id. Such an identifier may carry those values (see
 * {@link #getSortKey()}), so the edge can be read with a single column lookup instead of a scan over every edge
 * with that label on the vertex. The sort key is a locator, not part of the identity: two identifiers of the same
 * relation are equal whether or not they carry it.
 *
 * <p>String form: {@code relationId<d>outVertexId<d>typeId[<d>inVertexId[<d>K<sortKey>]]}, where {@code <d>} is
 * {@link #TOSTRING_DELIMITER} and {@code sortKey} is lowercase hex.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public final class RelationIdentifier implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationIdentifier.class);
    public static final String JANUSGRAPH_RELATION_DELIMITER = "JANUSGRAPH_RELATION_DELIMITER";
    public static final String TOSTRING_DELIMITER;

    /**
     * Prefix of the string token that carries the sort key. No other token can start with it: vertex and relation
     * ids are lowercase base36, and string vertex ids start with {@link LongEncoding#STRING_ENCODING_MARKER}.
     */
    public static final char SORT_KEY_MARKER = 'K';

    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    /**
     * Whether identifiers can carry a sort key. False only if {@link #TOSTRING_DELIMITER} is configured as
     * {@link #SORT_KEY_MARKER}, which would make the string form ambiguous.
     */
    public static final boolean SORT_KEY_SUPPORTED;

    private final Object outVertexId;
    private final long typeId;
    private final long relationId;
    private final Object inVertexId;
    /**
     * Lowercase hex of the edge's sort-key values in their column encoding, or {@code null}. Transient so that Java
     * and Gryo serialization keep their existing format; those paths carry the identifier without it.
     */
    private final transient String sortKey;

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
        SORT_KEY_SUPPORTED = TOSTRING_DELIMITER.indexOf(SORT_KEY_MARKER) < 0;
    }

    private RelationIdentifier() {
        outVertexId = null;
        typeId = 0;
        relationId = 0;
        inVertexId = null;
        sortKey = null;
    }

    public RelationIdentifier(final Object outVertexId, final long typeId, final long relationId, final Object inVertexId) {
        this(outVertexId, typeId, relationId, inVertexId, null);
    }

    private RelationIdentifier(final Object outVertexId, final long typeId, final long relationId,
                               final Object inVertexId, final String sortKey) {
        this.outVertexId = outVertexId;
        this.typeId = typeId;
        this.relationId = relationId;
        this.inVertexId = inVertexId;
        this.sortKey = sortKey;
    }

    /**
     * Returns a copy of this identifier that carries the given sort key.
     *
     * @param sortKey lowercase hex of the sort-key values in their column encoding
     * @throws IllegalArgumentException if this identifier is not an edge's, or the sort key is not valid hex
     * @throws IllegalStateException if {@link #SORT_KEY_SUPPORTED} is false
     */
    public RelationIdentifier withSortKey(final String sortKey) {
        if (!SORT_KEY_SUPPORTED) {
            throw new IllegalStateException("Sort keys are not supported with relation delimiter " + TOSTRING_DELIMITER);
        }
        if (inVertexId == null) {
            throw new IllegalArgumentException("Only edge identifiers can carry a sort key: " + this);
        }
        if (!isValidSortKey(sortKey)) {
            throw new IllegalArgumentException("Not a valid sort key: " + sortKey);
        }
        return new RelationIdentifier(outVertexId, typeId, relationId, inVertexId, sortKey);
    }

    /**
     * @return a copy of this identifier without a sort key, or this identifier if it carries none
     */
    public RelationIdentifier withoutSortKey() {
        return sortKey == null ? this : new RelationIdentifier(outVertexId, typeId, relationId, inVertexId, null);
    }

    /**
     * @return lowercase hex of the edge's sort-key values in their column encoding, or {@code null}
     */
    public String getSortKey() {
        return sortKey;
    }

    public boolean hasSortKey() {
        return sortKey != null;
    }

    public static boolean isValidSortKey(final String sortKey) {
        if (sortKey == null || sortKey.isEmpty() || sortKey.length() % 2 != 0) return false;
        for (int i = 0; i < sortKey.length(); i++) {
            final char c = sortKey.charAt(i);
            if ((c < '0' || c > '9') && (c < 'a' || c > 'f')) return false;
        }
        return true;
    }

    public static String encodeSortKey(final byte[] bytes) {
        final char[] chars = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            chars[2 * i] = HEX_DIGITS[(bytes[i] >> 4) & 0xF];
            chars[2 * i + 1] = HEX_DIGITS[bytes[i] & 0xF];
        }
        return new String(chars);
    }

    public static byte[] decodeSortKey(final String sortKey) {
        if (!isValidSortKey(sortKey)) {
            throw new IllegalArgumentException("Not a valid sort key: " + sortKey);
        }
        final byte[] bytes = new byte[sortKey.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) ((Character.digit(sortKey.charAt(2 * i), 16) << 4)
                | Character.digit(sortKey.charAt(2 * i + 1), 16));
        }
        return bytes;
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
            if (sortKey != null) {
                s.append(TOSTRING_DELIMITER).append(SORT_KEY_MARKER).append(sortKey);
            }
        }
        return s.toString();
    }

    public static RelationIdentifier parse(String id) {
        String[] elements = id.split(TOSTRING_DELIMITER);
        String sortKey = null;
        if (SORT_KEY_SUPPORTED && elements.length == 5 && !elements[4].isEmpty()
            && elements[4].charAt(0) == SORT_KEY_MARKER) {
            sortKey = elements[4].substring(1);
            if (!isValidSortKey(sortKey))
                throw new IllegalArgumentException("Not a valid relation identifier: " + id);
            elements = Arrays.copyOf(elements, 4);
        }
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
            return new RelationIdentifier(outVertexId, typeId, relationId, inVertexId, sortKey);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid id - each token expected to be a number", e);
        }
    }
}
