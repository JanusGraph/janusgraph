// Copyright 2018 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.io.graphson;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class JanusGraphSONModuleTest {

    @ParameterizedTest(name= "GraphSON Version: {0}")
    @EnumSource(value = GraphSONVersion.class, mode = EnumSource.Mode.EXCLUDE, names = { "V1_0" })
    public void testTinkerPopPredicatesAsGraphSON(GraphSONVersion graphSONVersion) throws Exception {
        Graph graph = EmptyGraph.instance();
        GraphTraversalSource g = graph.traversal();

        GraphTraversal[] traversals = {
            g.V().has("age", gt(13)),
            g.V().has("age", within(20, 29)),
            g.V().has("age", P.not(within(20, 29))) };

        graphsonSerializationTest(traversals, graphSONVersion);
    }

    @ParameterizedTest(name= "GraphSON Version: {0}")
    @EnumSource(value = GraphSONVersion.class, mode = EnumSource.Mode.EXCLUDE, names = { "V1_0" })
    public void testJanusGraphPredicatesAsGraphSON(GraphSONVersion graphSONVersion) throws Exception {
        Graph graph = EmptyGraph.instance();
        GraphTraversalSource g = graph.traversal();

        GraphTraversal[] traversals = {
            g.E().has("place", Geo.geoIntersect(Geoshape.circle(37.97, 23.72, 50))),
            g.E().has("place", Geo.geoWithin(Geoshape.circle(37.97, 23.72, 50))),
            g.E().has("place", Geo.geoDisjoint(Geoshape.circle(37.97, 23.72, 50))),
            g.V().has("place", Geo.geoContains(Geoshape.point(37.97, 23.72))),
            g.V().has("name", Text.textContains("neptune")), g.V().has("name", Text.textContainsPrefix("nep")),
            g.V().has("name", Text.textContainsRegex("nep.*")), g.V().has("name", Text.textPrefix("n")),
            g.V().has("name", Text.textRegex(".*n.*")), g.V().has("name", Text.textContainsFuzzy("neptun")),
            g.V().has("name", Text.textFuzzy("nepitne")) };

        graphsonSerializationTest(traversals, graphSONVersion);
    }

    private void graphsonSerializationTest(GraphTraversal[] traversals, GraphSONVersion version) throws Exception {
        final GraphSONMapper mapper = GraphSONMapper.build().version(version).typeInfo(TypeInfo.PARTIAL_TYPES).addRegistry(JanusGraphIoRegistry.instance()).create();
        final GraphSONWriter writer = GraphSONWriter.build().mapper(mapper).create();
        final GraphSONReader reader = GraphSONReader.build().mapper(mapper).create();

        for (GraphTraversal traversal : traversals) {
            Bytecode expectedBytecode = traversal.asAdmin().getBytecode();

            ByteArrayOutputStream serializationStream = new ByteArrayOutputStream();
            writer.writeObject(serializationStream, expectedBytecode);

            ByteArrayInputStream inputStream = new ByteArrayInputStream(serializationStream.toByteArray());

            Bytecode result = reader.readObject(inputStream, Bytecode.class);
            assertEquals(expectedBytecode, result);
        }
    }
}
