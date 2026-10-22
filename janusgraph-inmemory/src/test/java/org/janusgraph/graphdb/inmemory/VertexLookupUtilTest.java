// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.graphdb.inmemory;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.util.VertexLookupUtil;
import org.janusgraph.graphdb.transaction.VertexLookup;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.janusgraph.graphdb.transaction.VertexLookup.EXISTENCE;
import static org.janusgraph.graphdb.transaction.VertexLookup.LABEL;
import static org.janusgraph.graphdb.transaction.VertexLookup.LABEL_AND_PROPERTIES;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * What a traversal's lookup of vertices by id reads along with their existence, after JanusGraph's strategies: what the
 * folded has containers or the next step read of each vertex anyway, and nothing more.
 */
public class VertexLookupUtilTest {

    private static JanusGraph open(String... settings) {
        final JanusGraphFactory.Builder builder = JanusGraphFactory.build().set("storage.backend", "inmemory");
        for (int i = 0; i < settings.length; i += 2) {
            builder.set(settings[i], settings[i + 1]);
        }
        return builder.open();
    }

    //The lookup of the traversal's start step after JanusGraph's strategies: a GraphStep of ids, or a JanusGraphStep
    //into which hasId() folded them, along with the has containers it tests every vertex with
    private static VertexLookup lookupOf(JanusGraph graph, Function<GraphTraversalSource, GraphTraversal<?, ?>> traversal) {
        final Traversal.Admin<?, ?> admin = traversal.apply(graph.traversal()).asAdmin();
        admin.applyStrategies();
        final GraphStep<?, ?> start = (GraphStep<?, ?>) admin.getStartStep();
        final List<HasContainer> folded = start instanceof JanusGraphStep
            ? ((JanusGraphStep<?, ?>) start).getHasContainers() : Collections.emptyList();
        return VertexLookupUtil.lookupFor(start, folded, start.getIds().length,
            ((StandardJanusGraph) graph).getConfiguration().hasPropertyPrefetching());
    }

    private static void assertLookup(VertexLookup expected, JanusGraph graph,
                                     Function<GraphTraversalSource, GraphTraversal<?, ?>> traversal) {
        assertEquals(expected, lookupOf(graph, traversal), traversal.apply(graph.traversal()).toString());
    }

    @Test
    public void aLookupReadsWhatTheNextStepReadsOfEachVertex() {
        try (JanusGraph graph = open()) {
            assertLookup(EXISTENCE, graph, g -> g.V(1));
            assertLookup(EXISTENCE, graph, g -> g.V(1).id());
            assertLookup(EXISTENCE, graph, g -> g.V(1).out());
            assertLookup(EXISTENCE, graph, g -> g.V(1).outE("knows"));
            assertLookup(EXISTENCE, graph, g -> g.V(1).count());

            //All properties of each vertex
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).values());
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).properties());
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).valueMap());
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).elementMap());
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).propertyMap());
            //The label
            assertLookup(LABEL, graph, g -> g.V(1).label());
            assertLookup(LABEL, graph, g -> g.V(1).elementMap("name"));
            assertLookup(LABEL, graph, g -> g.V(1).valueMap("name").with(WithOptions.tokens));
            assertLookup(LABEL, graph, g -> g.V(1).valueMap("name").with(WithOptions.tokens, WithOptions.labels));
            //Some properties, which the step reads on its own, and tokens which need no read or go into no map
            assertLookup(EXISTENCE, graph, g -> g.V(1).values("name"));
            assertLookup(EXISTENCE, graph, g -> g.V(1).values("name", "age"));
            assertLookup(EXISTENCE, graph, g -> g.V(1).properties("name"));
            assertLookup(EXISTENCE, graph, g -> g.V(1).valueMap("name"));
            assertLookup(EXISTENCE, graph, g -> g.V(1).valueMap("name").with(WithOptions.tokens, WithOptions.ids));
            assertLookup(EXISTENCE, graph, g -> g.V(1).propertyMap("name").with(WithOptions.tokens));
            //A limit which the properties step took over reads that many properties alone, one after it all of them
            assertLookup(EXISTENCE, graph, g -> g.V(1).local(__.properties().limit(1)));
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).properties().limit(1));

            //The properties which a property traversal, such as SubgraphStrategy's, keeps are known to it alone
            assertLookup(EXISTENCE, graph, g -> g.withStrategies(SubgraphStrategy.build()
                .vertexProperties(__.hasNot("secret")).create()).V(1).valueMap());

            //The steps which profile() adds in between are no reads
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).valueMap().profile());
            assertLookup(LABEL, graph, g -> g.V(1, 2).label().profile());
            assertLookup(EXISTENCE, graph, g -> g.V(1).out().profile());

            //Several vertices, whose next step reads them in batches
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1, 2).valueMap());
            assertLookup(LABEL, graph, g -> g.V(1, 2).label());
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2).values("name"));
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2).out());

            //Ids which hasId() gave, the same
            assertLookup(EXISTENCE, graph, g -> g.V().hasId(1, 2).out());
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V().hasId(1).valueMap());
        }
    }

    @Test
    public void aLookupReadsWhatTheHasContainersAfterItRead() {
        try (JanusGraph graph = open()) {
            assertLookup(LABEL, graph, g -> g.V(1).hasLabel("person"));
            assertLookup(LABEL, graph, g -> g.V(1, 2).hasLabel("person").out());
            //A property of each vertex, which the batches of the has step read with all others with query.fast-property
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).has("name", "john"));
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1, 2).has("name", "john").out());
            //The id is no read
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2).hasId(1));
            //A test of the label or the id beside it may leave a vertex out before its properties are read
            assertLookup(LABEL, graph, g -> g.V(1).hasLabel("person").has("name", "john"));
            assertLookup(LABEL, graph, g -> g.V(1).has("name", "john").hasLabel("person"));
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2).hasId(1).has("name", "john"));
            //The containers which a lookup of ids that hasId() gave folded in, which it tests every vertex with
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V().hasId(1).has("name", "john"));
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V().hasId(1, 2).has("name", "john").out());
            assertLookup(LABEL, graph, g -> g.V().hasId(1, 2).hasLabel("person"));
            assertLookup(LABEL, graph, g -> g.V().hasId(1).hasLabel("person").has("name", "john"));
            //which decide alone, as a vertex which fails them never reaches the next step
            assertLookup(LABEL, graph, g -> g.V().hasId(1).hasLabel("person").valueMap());
            assertLookup(LABEL, graph, g -> g.V().hasId(1, 2).hasLabel("person").values());
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V().hasId(1).has("name", "john").valueMap());
        }
        //Without query.fast-property a has step reads its keys alone, and the label
        try (JanusGraph graph = open("query.fast-property", "false")) {
            assertLookup(EXISTENCE, graph, g -> g.V(1).has("name", "john"));
            assertLookup(LABEL, graph, g -> g.V(1).hasLabel("person").has("name", "john"));
            assertLookup(EXISTENCE, graph, g -> g.V().hasId(1).has("name", "john"));
            assertLookup(EXISTENCE, graph, g -> g.V().hasId(1).has("name", "john").valueMap());
        }
        //unless its batches read all properties of its vertices
        try (JanusGraph graph = open("query.fast-property", "false", "query.batch.has-step-mode", "all_properties")) {
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).has("name", "john"));
            assertLookup(LABEL, graph, g -> g.V(1).hasLabel("person").has("name", "john"));
        }
    }

    //A has step whose batches read the properties the next step asks for too, or all of them where it asks for all or
    //where it isn't known what it asks for, and a has step which tests one vertex after the other
    @Test
    public void aLookupReadsWhatTheHasStepReadsInEachMode() {
        try (JanusGraph graph = open("query.fast-property", "false",
                "query.batch.has-step-mode", "required_and_next_properties")) {
            assertLookup(EXISTENCE, graph, g -> g.V(1).has("name", "john").values("age"));
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).has("name", "john").valueMap());
            assertLookup(EXISTENCE, graph, g -> g.V(1).has("name", "john").out());
        }
        try (JanusGraph graph = open("query.fast-property", "false",
                "query.batch.has-step-mode", "required_and_next_properties_or_all")) {
            assertLookup(EXISTENCE, graph, g -> g.V(1).has("name", "john").values("age"));
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).has("name", "john").valueMap());
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).has("name", "john").out());
        }
        //The first property tested of a vertex reads all of them with query.fast-property
        try (JanusGraph graph = open("query.batch.has-step-mode", "none")) {
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).has("name", "john"));
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2).has("name", "john"));
        }
        try (JanusGraph graph = open("query.fast-property", "false", "query.batch.has-step-mode", "none")) {
            assertLookup(EXISTENCE, graph, g -> g.V(1).has("name", "john"));
        }
    }

    //Without batches, a step reads its vertices one after the other, and may stop before the last of several
    @Test
    public void aLookupOfSeveralIdsReadsOnlyWhatAStepReadsInBatches() {
        try (JanusGraph graph = open("query.batch.enabled", "false")) {
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).valueMap());
            assertLookup(LABEL, graph, g -> g.V(1).label());
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).has("name", "john"));
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2).valueMap());
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2).label());
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2).has("name", "john"));
            //The step which looks the ids up tests every vertex itself
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V().hasId(1, 2).has("name", "john").valueMap());
        }
        try (JanusGraph graph = open("query.batch.label-step-mode", "none")) {
            assertLookup(LABEL, graph, g -> g.V(1).label());
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2).label());
        }
    }

    //Without query.batch.limited no barrier holds the vertices back: the multi-query step registers all of them first
    @Test
    public void aLookupOfSeveralIdsWithoutLimitedBatchesReadsWhatTheNextStepReads() {
        try (JanusGraph graph = open("query.batch.limited", "false")) {
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1, 2, 3).valueMap());
            assertLookup(LABEL, graph, g -> g.V(1, 2, 3).label());
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1, 2, 3).has("name", "john"));
        }
    }

    //A barrier or a batch smaller than the ids lets the step after it read the first vertices before the last
    @Test
    public void aLookupOfMoreIdsThanABatchHoldsReadsTheExistenceAlone() {
        try (JanusGraph graph = open()) {
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1, 2, 3).barrier(3).valueMap());
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2, 3).barrier(2).valueMap());
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).barrier(1).valueMap());
        }
        try (JanusGraph graph = open("query.batch.limited-size", "2")) {
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1, 2).valueMap());
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2, 3).valueMap());
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2, 3).label());
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2, 3).has("name", "john"));
        }
    }

    //In batches of all properties, a step which asks for two keys or more reads them all, and one key alone
    @Test
    public void aLookupReadsAllPropertiesWhereTheStepsBatchesDo() {
        try (JanusGraph graph = open("query.batch.properties-mode", "all_properties")) {
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).values("name", "age"));
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1, 2).valueMap("name", "age"));
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1, 2).elementMap("name", "age"));
            assertLookup(EXISTENCE, graph, g -> g.V(1).values("name"));
            assertLookup(EXISTENCE, graph, g -> g.V(1).values("name", "name"));
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2).valueMap("name"));
            assertLookup(LABEL, graph, g -> g.V(1, 2).elementMap("name"));
        }
        try (JanusGraph graph = open("query.batch.properties-mode", "none")) {
            assertLookup(LABEL_AND_PROPERTIES, graph, g -> g.V(1).values());
            assertLookup(EXISTENCE, graph, g -> g.V(1).values("name"));
            assertLookup(EXISTENCE, graph, g -> g.V(1, 2).values());
        }
    }
}
