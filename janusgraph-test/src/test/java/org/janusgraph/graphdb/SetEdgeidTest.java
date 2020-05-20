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

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Edge;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.tinkerpop.gremlin.structure.T;
import org.janusgraph.util.encoding.LongEncoding;

import org.janusgraph.graphdb.tinkerpop.JanusGraphTraversalSource;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SetEdgeidTest {

    StandardJanusGraph graph;

    public void initialize() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        config.set(GraphDatabaseConfiguration.STORAGE_READONLY,false);
        config.set(GraphDatabaseConfiguration.ALLOW_SETTING_VERTEX_ID, true);
        config.set(GraphDatabaseConfiguration.ALLOW_SETTING_EDGE_ID, true);
        graph = (StandardJanusGraph) JanusGraphFactory.open(config);
    }

    @AfterEach
    public void shutdown() {
        graph.close();
    }


    @Test
    public void graph_add() {
        initialize();

        JanusGraphTransaction tx = graph.newTransaction();
        try {
            tx.addVertex();
            fail();
        } catch (Exception ignored) {
        } finally {
            tx.rollback();
        }

        try {
            // graph.addVertex();
            System.out.println("graph add element");
            GraphTraversalSource g = graph.traversal();
            System.out.println(String.format("26833093692655552l encode result: %s", LongEncoding.encode(26833093692655552l)));
            System.out.println(String.format("20742965286657360l encode result: %s", LongEncoding.encode(20742965286657360l)));
            System.out.println("-----------------graph add element addVertex1-----------------");
            JanusGraphVertex jiulong = graph.addVertex(T.id, 26833093692655552l, T.label, "user", "name", "jiulong");
            JanusGraphVertex lan = graph.addVertex(T.id, 20742965286657360l, T.label, "user");

            System.out.println("-----------------graph add element addEdge1-----------------");
            JanusGraphEdge one = jiulong.addEdge("knows", lan, "user", "1", "year", "1993");
            System.out.println(String.format("first edge id: %s", one.id()));

            System.out.println("-----------------graph add element addEdge2-----------------");
            JanusGraphEdge two = jiulong.addEdge("get", "knows", lan, "year", "1993");
            System.out.println(String.format("second edge id: %s", two.id()));
            // System.out.println(g.E("1-7c7jd2enpog-2dx-5o8rmp86lao").valueMap().toList());

            System.out.println("-----------------graph add element addEdge3-----------------");
            JanusGraphEdge three = jiulong.addEdge("get", "knows", lan, "year", "1995");
            System.out.println(String.format("third edge id: %s", three.id()));
            System.out.println(g.E("get-7c7jd2enpog-2dx-5o8rmp86lao").valueMap().toList());
            
            System.out.println("-----------------graph add element queryEdge-----------------");
            System.out.println(g.V().has("name", "jiulong").outE("knows").valueMap().toList());
            // fail();
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            graph.tx().rollback();
        }

    }

    @Test
    public void traversal_add() {
        initialize();

        JanusGraphTransaction tx = graph.newTransaction();
        try {
            tx.addVertex();
            fail();
        } catch (Exception ignored) {
        } finally {
            tx.rollback();
        }

        try {
            // graph.addVertex();
            System.out.println("traveral add element");
            JanusGraphTraversalSource g = graph.JanusTraversal();
            System.out.println(String.format("26833093692655552l encode result: %s", LongEncoding.encode(26833093692655552l)));
            System.out.println(String.format("20742965286657360l encode result: %s", LongEncoding.encode(20742965286657360l)));
            System.out.println("-----------------traveral add element addVertex1-----------------");
            Vertex jiulong = g.addV("user").property(T.id, 26833093692655552l).property("name", "jiulong").property("userid", 7).next();
            
            System.out.println("-----------------traveral add element addVertex2-----------------");
            Vertex lan = g.addV("user").property(T.id, 20742965286657360l).property("userid", 9).next();

            System.out.println("-----------------traveral add element addEdge1-----------------");
            Edge one = g.addE("get", "friend").from(jiulong).to(lan).property("time", 1995).property("relation", "great").next();
            System.out.println(String.format("first edge id: %s", one.id()));

            System.out.println("-----------------traveral add element addEdge2-----------------");
            Edge two = g.addE("get", "friend").from(jiulong).to(lan).property("time", 1993).property("relation", "great").next();
            System.out.println(String.format("second edge id: %s", two.id()));

            System.out.println("-----------------traveral add element queryEdge-----------------");
            System.out.println(g.V().has("name", "jiulong").outE("friend").valueMap().toList());

            // fail();
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            graph.tx().rollback();
        }

    }


}