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

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class IDUtilsTest {
    @Test
    public void testCheckVertexId() {
        IDUtils.checkVertexId("a-string-id");
        IDUtils.checkVertexId(1);
        assertThrows(IllegalArgumentException.class, () -> IDUtils.checkVertexId(null));
        assertThrows(IllegalArgumentException.class, () -> IDUtils.checkVertexId(-1));
        assertThrows(IllegalArgumentException.class, () -> IDUtils.checkVertexId(0));
        assertThrows(IllegalArgumentException.class, () -> IDUtils.checkVertexId(UUID.randomUUID()));
    }
}
