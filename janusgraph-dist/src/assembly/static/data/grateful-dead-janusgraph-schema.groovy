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
 * gremlin> t = JanusGraphFactory.open('conf/janusgraph-cassandra.properties')
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
    m.buildEdgeIndex(followedBy, "followedByWeight", Direction.BOTH, Order.decr, weight)
    m.commit()
}
