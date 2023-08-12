// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop;

import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.janusgraph.core.attribute.Geoshape;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.util.ser.AbstractMessageSerializer.TOKEN_IO_REGISTRIES;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class JanusGraphIoRegistryTest {
    @Test
    public void testTokenIoRegistyInConfig() throws SerializationException {
        final GraphSONMessageSerializerV3 serializer = new GraphSONMessageSerializerV3();
        final Map<String,Object> config = new HashMap<>();
        config.put(TOKEN_IO_REGISTRIES, Collections.singletonList(JanusGraphIoRegistry.class.getName()));
        serializer.configure(config, Collections.emptyMap());

        GraphTraversal traversal = EmptyGraph.instance().traversal().addV().property("loc", Geoshape.point(1.0f, 1.0f));
        Bytecode expectedBytecode = traversal.asAdmin().getBytecode();

        String serializedMessage = serializer.serializeRequestAsString(
            RequestMessage.build(Tokens.OPS_BYTECODE).processor("traversal")
            .addArg(Tokens.ARGS_GREMLIN, expectedBytecode).create(), ByteBufAllocator.DEFAULT);

        RequestMessage requestMessage1 = serializer.deserializeRequest(serializedMessage);
        Bytecode result = (Bytecode)requestMessage1.getArgs().get(Tokens.ARGS_GREMLIN);
        assertEquals(expectedBytecode, result);
    }
}
