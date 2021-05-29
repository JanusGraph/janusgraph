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
import org.janusgraph.graphdb.tinkerpop.JanusGraphPSerializer;
import org.janusgraph.graphdb.tinkerpop.io.JanusGraphP;

import java.io.IOException;

public class JanusGraphPBinarySerializer extends JanusGraphTypeSerializer<JanusGraphP> {
    public JanusGraphPBinarySerializer() {
        super(GraphBinaryType.JanusGraphP);
    }

    @Override
    public JanusGraphP readNonNullableValue(Buffer buffer, GraphBinaryReader context) throws IOException {
        String predicateName = context.readValue(buffer, String.class, false);
        Object value = context.read(buffer);
        return JanusGraphPSerializer.createPredicateWithValue(predicateName, value);
    }

    @Override
    protected void writeNonNullableValue(JanusGraphP value, Buffer buffer, GraphBinaryWriter context) throws IOException {
        String predicateName = value.getBiPredicate().toString();
        context.writeValue(predicateName, buffer, false);
        Object arg = value.getValue();
        context.write(arg, buffer);
    }
}
