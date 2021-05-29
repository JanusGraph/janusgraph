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

package org.janusgraph.graphdb.serializer;

import org.janusgraph.StorageSetup;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.jupiter.api.Test;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class EdgeSerializerTest {

    @Test
    public void testValueOrdering() {
        StandardJanusGraph graph = (StandardJanusGraph) StorageSetup.getInMemoryGraph();
        JanusGraphManagement management = graph.openManagement();
        management.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        for (int i=1;i<=5;i++) management.makePropertyKey("key" + i).dataType(Integer.class).make();
        management.commit();

        JanusGraphVertex v1 = graph.addVertex(), v2 = graph.addVertex();
        JanusGraphEdge e1 = v1.addEdge("father",v2);
        for (int i=1;i<=5;i++) e1.property("key"+i,i);

        graph.tx().commit();

        e1.remove();
        graph.tx().commit();

    }

}
