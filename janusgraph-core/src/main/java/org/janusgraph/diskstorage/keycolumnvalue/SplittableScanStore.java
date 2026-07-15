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

package org.janusgraph.diskstorage.keycolumnvalue;

import org.janusgraph.diskstorage.BackendException;

/**
 * Optional capability of a {@link KeyColumnValueStore} whose unordered full scan can be split into
 * disjoint key-space partitions that are scanned independently (and therefore concurrently).
 * <p>
 * Contract, for a fixed {@code splitCount}:
 * <ul>
 *   <li>the key sets of splits {@code 0..splitCount-1} are pairwise disjoint and their union is
 *       exactly the key set of the corresponding unsplit {@link KeyColumnValueStore#getKeys(SliceQuery,
 *       StoreTransaction)} scan (no gaps, no overlaps);</li>
 *   <li>within one split, every {@link SliceQuery} iterates keys in the same order, so per-key results
 *       of multiple queries over the same split can be merged by walking the iterators side by side
 *       (same guarantee the store already provides for whole-table scans used by scan jobs).</li>
 * </ul>
 * The scan-job framework uses this to run one row-collection pipeline per split in parallel, which
 * removes the single-scan-thread ceiling on backends (like CQL) whose full scan is otherwise one
 * paged query.
 */
public interface SplittableScanStore {

    /**
     * Number of splits the store is configured to use for parallel unordered scans.
     *
     * @return the split count; a value of 1 (or less) means splitting is disabled or unsupported and
     *         callers must fall back to {@link KeyColumnValueStore#getKeys(SliceQuery, StoreTransaction)}
     */
    int getUnorderedScanSplitCount();

    /**
     * Unordered scan restricted to split {@code splitIndex} out of {@code splitCount}.
     *
     * @param query      slice of each row to fetch
     * @param txh        enclosing store transaction
     * @param splitIndex index of the requested split, in {@code [0, splitCount)}
     * @param splitCount total number of splits the key space is divided into
     * @return iterator over the keys of this split only
     */
    KeyIterator getKeysForSplit(SliceQuery query, StoreTransaction txh, int splitIndex, int splitCount) throws BackendException;
}
