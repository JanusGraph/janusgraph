// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.graphdb;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.junit.Test;

public abstract class GetEdgeByHasIdTest extends JanusGraphBaseTest {

    @Test
    public void getEdgeByHasId() {
        GraphTraversalSource g = graph.traversal();

        g.addV("community").property("name","first").as("firstCommunity").
            addV("community").property("name","second").as("secondCommunity").
            addV("person").addE("follows").to("firstCommunity").
            addV("person").addE("follows").to("firstCommunity").
            addV("person").addE("follows").to("secondCommunity").iterate();

        RelationIdentifier eid = (RelationIdentifier) g.E().id().next();

        GraphTraversal<Edge, Edge> t1 = g.E().hasId(eid);
        GraphTraversal<Edge, Edge> t2 = g.E(eid);


        Edge e1 = t1.next();

        Edge e2 = t2.next();

        assert e1 != null && e1.equals(e2);

    }
}
