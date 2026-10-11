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

package org.janusgraph;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.cql.CQLConfigOptions;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A transaction which looks vertices up by id, reads their properties together, changes an indexed property of each
 * and commits; the time is that of the whole transaction. The changes read nothing - the batch loaded what they read -
 * and the vertices' labels haven't been read, unless {@code labelsLoaded}. With an index restricted to the vertices'
 * label ({@code labelRestricted}), the commit checks the label of every vertex the index updates are for; without one,
 * it checks none. Each transaction gives the vertices one of two names, so that the index doesn't grow. It expects a
 * Cassandra at {@code cqlHost}:{@code cqlPort} whose datacenter is {@code cqlDatacenter}, and recreates the keyspace
 * {@code cqlKeyspace}.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CQLCommitIndexUpdatesBenchmark {

    @Param({"100", "1000"})
    int vertices;

    @Param({"true", "false"})
    boolean labelRestricted;

    @Param({"false", "true"})
    boolean labelsLoaded;

    @Param({"localhost"})
    String cqlHost;

    @Param({"9042"})
    int cqlPort;

    @Param({"dc1"})
    String cqlDatacenter;

    @Param({"janusgraph_commit_index_updates_benchmark"})
    String cqlKeyspace;

    JanusGraph graph;
    Object[] vertexIds;
    long round;

    public WriteConfiguration getConfiguration() {
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "cql");
        config.set(GraphDatabaseConfiguration.STORAGE_HOSTS, new String[]{cqlHost});
        config.set(GraphDatabaseConfiguration.STORAGE_PORT, cqlPort);
        config.set(CQLConfigOptions.LOCAL_DATACENTER, cqlDatacenter);
        config.set(CQLConfigOptions.KEYSPACE, cqlKeyspace);
        return config.getConfiguration();
    }

    @Setup
    public void setUp() throws BackendException {
        // A keyspace an earlier run left behind, if it was cut short, is dropped first
        JanusGraphFactory.drop(JanusGraphFactory.open(getConfiguration()));
        graph = JanusGraphFactory.open(getConfiguration());
        final JanusGraphManagement mgmt = graph.openManagement();
        final VertexLabel person = mgmt.makeVertexLabel("person").make();
        final PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        final JanusGraphManagement.IndexBuilder index = mgmt.buildIndex("byName", Vertex.class).addKey(name);
        if (labelRestricted) {
            index.indexOnly(person);
        }
        index.buildCompositeIndex();
        mgmt.commit();
        final JanusGraphTransaction tx = graph.newTransaction();
        vertexIds = new Object[vertices];
        for (int i = 0; i < vertices; i++) {
            vertexIds[i] = tx.addVertex("person").property("name", "person " + i).element().id();
        }
        tx.commit();
    }

    @TearDown
    public void tearDown() throws BackendException {
        JanusGraphFactory.drop(graph);
    }

    @Benchmark
    public void commitIndexedChanges() {
        final JanusGraphTransaction tx = graph.buildTransaction().checkExternalVertexExistence(false).start();
        final List<JanusGraphVertex> people = new ArrayList<>(vertexIds.length);
        for (Object id : vertexIds) {
            people.add(tx.getVertex(id));
        }
        tx.multiQuery(people).properties();
        if (labelsLoaded) {
            // In a batch too: the commit then finds every label loaded
            tx.traversal().V(vertexIds).label().iterate();
        }
        final String suffix = (round++ & 1) == 0 ? " even" : " odd";
        for (JanusGraphVertex person : people) {
            person.property("name", "person " + person.id() + suffix);
        }
        tx.commit();
    }
}
