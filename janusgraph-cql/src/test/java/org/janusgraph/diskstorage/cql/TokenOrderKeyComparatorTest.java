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

package org.janusgraph.diskstorage.cql;

import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pins the two layers of {@link TokenOrderKeyComparator} in isolation, with a stubbed
 * {@link TokenMap}: keys order by token first, and keys with EQUAL tokens (hash collisions, which
 * real Murmur3 data will essentially never produce in a test, so the integration tests cannot
 * exercise this path) tie-break by the UNSIGNED key bytes - Cassandra's {@code DecoratedKey}
 * order. A signed tie-break would misclassify rows as stale in the scan merge join whenever a
 * collision straddled the 0x80 byte boundary.
 */
public class TokenOrderKeyComparatorTest {

    private static final StaticBuffer KEY_01 = StaticArrayBuffer.of(new byte[]{0x01});
    private static final StaticBuffer KEY_01_00 = StaticArrayBuffer.of(new byte[]{0x01, 0x00});
    private static final StaticBuffer KEY_FF = StaticArrayBuffer.of(new byte[]{(byte) 0xFF});

    /** Compares like the driver's Murmur3 token: by a single long. */
    private static final class StubToken implements Token {
        private final long value;

        StubToken(long value) {
            this.value = value;
        }

        @Override
        public int compareTo(Token other) {
            return Long.compare(value, ((StubToken) other).value);
        }
    }

    private static TokenOrderKeyComparator comparatorWithTokens(Map<StaticBuffer, Long> tokens) {
        final TokenMap tokenMap = mock(TokenMap.class);
        when(tokenMap.newToken(any())).thenAnswer(invocation -> {
            final ByteBuffer keyBuffer = (ByteBuffer) invocation.getArguments()[0];
            final Long token = tokens.get(StaticArrayBuffer.of(keyBuffer.duplicate()));
            assertNotNull(token, "token requested for an unexpected key");
            return new StubToken(token);
        });
        return new TokenOrderKeyComparator(tokenMap);
    }

    @Test
    public void keysOrderByTokenBeforeKeyBytes() {
        final Map<StaticBuffer, Long> tokens = new HashMap<>();
        tokens.put(KEY_FF, 10L);
        tokens.put(KEY_01, 20L);
        final TokenOrderKeyComparator comparator = comparatorWithTokens(tokens);

        // 0xFF > 0x01 as key bytes, but its token is smaller, and the token must dominate.
        assertTrue(comparator.compare(KEY_FF, KEY_01) < 0);
        assertTrue(comparator.compare(KEY_01, KEY_FF) > 0);
    }

    @Test
    public void equalTokensTieBreakByUnsignedKeyBytes() {
        final Map<StaticBuffer, Long> tokens = new HashMap<>();
        tokens.put(KEY_01, 7L);
        tokens.put(KEY_FF, 7L);
        final TokenOrderKeyComparator comparator = comparatorWithTokens(tokens);

        // A SIGNED byte comparison would put 0xFF (-1) before 0x01; DecoratedKey order is unsigned.
        assertTrue(comparator.compare(KEY_01, KEY_FF) < 0);
        assertTrue(comparator.compare(KEY_FF, KEY_01) > 0);
    }

    @Test
    public void equalTokensPrefixKeySortsBeforeItsExtension() {
        final Map<StaticBuffer, Long> tokens = new HashMap<>();
        tokens.put(KEY_01, 7L);
        tokens.put(KEY_01_00, 7L);
        final TokenOrderKeyComparator comparator = comparatorWithTokens(tokens);

        assertTrue(comparator.compare(KEY_01, KEY_01_00) < 0);
        assertTrue(comparator.compare(KEY_01_00, KEY_01) > 0);
    }

    @Test
    public void sameKeyComparesEqual() {
        final Map<StaticBuffer, Long> tokens = new HashMap<>();
        tokens.put(KEY_01, 7L);
        final TokenOrderKeyComparator comparator = comparatorWithTokens(tokens);

        assertEquals(0, comparator.compare(KEY_01, KEY_01));
    }
}
