// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.io.binary;

import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.janusgraph.graphdb.relations.RelationIdentifier;

import java.io.IOException;

public class RelationIdentifierGraphBinarySerializer extends JanusGraphTypeSerializer<RelationIdentifier> {
    public RelationIdentifierGraphBinarySerializer() {
        super(GraphBinaryType.RelationIdentifier);
    }

    @Override
    public RelationIdentifier readNonNullableValue(Buffer buffer, GraphBinaryReader context) throws IOException {
        final long outVertexId = buffer.readLong();
        final long typeId = buffer.readLong();
        final long relationId = buffer.readLong();
        final long inVertexId = buffer.readLong();
        return new RelationIdentifier(outVertexId, typeId, relationId, inVertexId);
    }

    @Override
    protected void writeNonNullableValue(RelationIdentifier value, Buffer buffer, GraphBinaryWriter context) throws IOException {
        buffer.writeLong(value.getOutVertexId());
        buffer.writeLong(value.getTypeId());
        buffer.writeLong(value.getRelationId());
        buffer.writeLong(value.getInVertexId());
    }
}
