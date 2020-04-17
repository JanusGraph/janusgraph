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

/* janusgraph-schema.groovy
 *
 * Helper functions for declaring JanusGraph schema elements
 * (vertex labels, edge labels, property keys) to accommodate
 * TP3 sample data.
 *
 * Sample usage in a gremlin.sh session:
 *
 * gremlin> :load data/janusgraph-schema-grateful-dead.groovy
 * ==>true
 * ==>true
 * gremlin> t = JanusGraphFactory.open('conf/janusgraph-cql.properties')
 * ==>standardjanusgraph[cassandrathrift:[127.0.0.1]]
 * gremlin> defineGratefulDeadSchema(t)
 * ==>null
 * gremlin> t.close()
 * ==>null
 * gremlin>
 */

def defineGratefulDeadSchema(janusGraph) {
    m = janusGraph.openManagement()
    // vertex labels
    artist = m.makeVertexLabel("artist").make()
    song   = m.makeVertexLabel("song").make()
    // edge labels
    sungBy     = m.makeEdgeLabel("sungBy").make()
    writtenBy  = m.makeEdgeLabel("writtenBy").make()
    followedBy = m.makeEdgeLabel("followedBy").make()
    // vertex and edge properties
    blid         = m.makePropertyKey("bulkLoader.vertex.id").dataType(Long.class).make()
    name         = m.makePropertyKey("name").dataType(String.class).make()
    songType     = m.makePropertyKey("songType").dataType(String.class).make()
    performances = m.makePropertyKey("performances").dataType(Integer.class).make()
    weight       = m.makePropertyKey("weight").dataType(Integer.class).make()
    // global indices
    m.buildIndex("byBulkLoaderVertexId", Vertex.class).addKey(blid).buildCompositeIndex()
    m.buildIndex("artistsByName", Vertex.class).addKey(name).indexOnly(artist).buildCompositeIndex()
    m.buildIndex("songsByName", Vertex.class).addKey(name).indexOnly(song).buildCompositeIndex()
    // vertex centric indices
    m.buildEdgeIndex(followedBy, "followedByWeight", Direction.BOTH, Order.desc, weight)
    m.commit()
}
