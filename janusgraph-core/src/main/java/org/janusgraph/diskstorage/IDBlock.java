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

package org.janusgraph.diskstorage;

/**
 * Represents a block of ids. {@link #numIds()} return how many ids are in this block and {@link #getId(long)} retrieves
 * the id at the given position, where the position must be smaller than the number of ids in this block (similar to array access).
 * <p>
 * Any IDBlock implementation must be completely thread-safe.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IDBlock {

    /**
     * Number of ids in this block.
     *
     * @return
     */
    long numIds();

    /**
     * Returns the id at the given index. Index must be non-negative and smaller than {@link #numIds()}.
     *
     * @param index
     * @return
     */
    long getId(long index);

}
