// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.diskstorage.util;

public class ArrayUtil {
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    /**
     * Given an old capacity of an array, generate the new capacity that is larger than or equal to minimum required capacity.
     * Under most circumstances, it doubles the old capacity. If the old capacity is already larger than half of @{link #MAX_ARRAY_SIZE},
     * then new capacity will be {@link #MAX_ARRAY_SIZE}, which is slightly smaller than Integer.MAX_VALUE to avoid
     * Out of Memory error. If the min capacity is smaller than old capacity, then old capacity will be simply returned.
     * @param oldCapacity old capacity
     * @param minCapacity minimum desired capacity
     * @return a new capacity that is larger than or equal to minCapacity
     * @throws IllegalArgumentException if minimum required capacity is larger than {@link #MAX_ARRAY_SIZE}, or minimum
     * required capacity is negative (likely caused by integer overflow)
     */
    public static int growSpace(int oldCapacity, int minCapacity) {
        if (minCapacity < 0) {
            throw new IllegalArgumentException(String.format("required capacity %d is negative, likely caused by integer overflow", minCapacity));
        }
        if (minCapacity <= oldCapacity) {
            return oldCapacity;
        }
        if (minCapacity > MAX_ARRAY_SIZE) {
            throw new IllegalArgumentException(String.format("required capacity %d is larger than MAX_ARRAY_SIZE [%d]",
                minCapacity, MAX_ARRAY_SIZE));
        }
        int newCapacity = oldCapacity << 1;
        if (newCapacity - minCapacity < 0) {
            newCapacity = minCapacity;
        } else if (newCapacity - MAX_ARRAY_SIZE > 0) {
            newCapacity = MAX_ARRAY_SIZE;
        }
        return newCapacity;
    }

}
