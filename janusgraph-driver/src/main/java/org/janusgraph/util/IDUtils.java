// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.util;

public class IDUtils {
    public static int compare(Object id1, Object id2) {
        if (id1 instanceof Number && id2 instanceof String) return -1;
        if (id1 instanceof String && id2 instanceof Number) return 1;
        if (id1 instanceof String && id2 instanceof String) {
            return ((String) id1).compareTo((String) id2);
        }
        if (id1 instanceof Number && id2 instanceof Number) {
            return Long.compare(((Number) id1).longValue(), ((Number) id2).longValue());
        }
        throw new IllegalArgumentException("Cannot compare ids: " + id1 + ", " + id2);
    }

    public static void checkId(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null");
        }
        if (id instanceof String) {
            return;
        }
        if (id instanceof Number) {
            if (((Number) id).longValue() <= 0) {
                throw new IllegalArgumentException(String.format("Id %d is non-positive", ((Number) id).longValue()));
            }
            return;
        }
        throw new IllegalArgumentException("Id must be either String or a positive long value");
    }
}
