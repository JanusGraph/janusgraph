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
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;

import java.util.Comparator;

/**
 * The ring order a CQL range scan iterates partition keys in, computed client-side: by the
 * partitioner's token first and, among keys with equal tokens (hash collisions), by the unsigned
 * key bytes - Cassandra's {@code DecoratedKey} order. Token computation is delegated to the
 * driver's {@link TokenMap} (the same token factory the driver relies on for token-aware routing),
 * so it matches the connected cluster's partitioner by construction. Only the token FACTORY is
 * used, which is fixed for the lifetime of a cluster, so holding one {@link TokenMap} instance
 * across ring topology changes is safe.
 * <p>
 * Declared through {@link StoreFeatures#getScanKeyOrder()}, this order puts multi-query scans
 * (e.g. a reindex) on the lossless merge-join strategy. The scan framework verifies the order
 * against every scan as it consumes rows and fails the scan on the first violation, so a
 * client/server order mismatch surfaces as an error instead of silently dropped rows.
 */
public class TokenOrderKeyComparator implements Comparator<StaticBuffer> {

    private final TokenMap tokenMap;

    public TokenOrderKeyComparator(final TokenMap tokenMap) {
        this.tokenMap = tokenMap;
    }

    @Override
    public int compare(final StaticBuffer a, final StaticBuffer b) {
        final Token tokenA = tokenMap.newToken(a.asByteBuffer());
        final Token tokenB = tokenMap.newToken(b.asByteBuffer());
        final int byToken = tokenA.compareTo(tokenB);
        return byToken != 0 ? byToken : a.compareTo(b);
    }

    /** Identifies this order (and the partitioner it mirrors) in scan-order violation errors. */
    @Override
    public String toString() {
        return "CQL token order (partitioner " + tokenMap.getPartitionerName() + ")";
    }
}
