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
    public static void checkVertexId(Object vertexId) {
        if (vertexId == null) {
            throw new IllegalArgumentException("vertex id cannot be null");
        }
        if (vertexId instanceof String) {
            // string-type vertex id is allowed
            return;
        }
        if (vertexId instanceof Number) {
            if (((Number) vertexId).longValue() <= 0) {
                throw new IllegalArgumentException(String.format("vertex id %d is non-positive", ((Number) vertexId).longValue()));
            }
            return;
        }
        throw new IllegalArgumentException("vertex id must be either String or a positive long value");
    }
}
