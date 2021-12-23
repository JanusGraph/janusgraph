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

package org.janusgraph.graphdb.relations;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RelationIdentifierTest {

    @Test
    public void testDisallowStringVertexId() {
        long outVertexId = 12135151000L;
        long inVertexId = 21423513000L;
        long typeId = 123;
        long relationId = 500;
        RelationIdentifier rId = new RelationIdentifier(outVertexId, typeId, relationId, inVertexId);
        String serialized = rId.toString();
        RelationIdentifier deserialized = RelationIdentifier.parse(serialized);
        assertEquals(rId, deserialized);
        assertEquals(outVertexId, deserialized.getOutVertexId());
        assertEquals(inVertexId, deserialized.getInVertexId());
        assertEquals(typeId, deserialized.getTypeId());
        assertEquals(relationId, deserialized.getRelationId());
    }

    @Test
    public void testAllowStringVertexId() {
        String outVertexId = UUID.randomUUID().toString().replace('-', '_');
        long inVertexId = Long.MAX_VALUE;
        long typeId = 123;
        long relationId = 500;
        RelationIdentifier rId = new RelationIdentifier(outVertexId, typeId, relationId, inVertexId);
        String serialized = rId.toString();
        RelationIdentifier deserialized = RelationIdentifier.parse(serialized);
        assertEquals(rId, deserialized);
        assertEquals(outVertexId, deserialized.getOutVertexId());
        assertEquals(inVertexId, deserialized.getInVertexId());
        assertEquals(typeId, deserialized.getTypeId());
        assertEquals(relationId, deserialized.getRelationId());
    }
}
