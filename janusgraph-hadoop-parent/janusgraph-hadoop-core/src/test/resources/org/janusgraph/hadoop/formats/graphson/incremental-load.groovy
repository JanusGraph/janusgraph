def JanusGraphVertex getOrCreateVertex(FaunusVertex faunusVertex, JanusGraph graph, TaskInputOutputContext context, Logger log) {
    String uniqueKey = "name";
    Object uniqueValue = faunusVertex.value(uniqueKey);
    Vertex janusgraphVertex;
    if (null == uniqueValue)
      throw new RuntimeException("The provided Faunus vertex does not have a property for the unique key: " + faunusVertex);
  
    Iterator<Vertex> itty = graph.query().has(uniqueKey, uniqueValue).vertices().iterator();
    if (itty.hasNext()) {
      janusgraphVertex = itty.next();
      if (itty.hasNext())
        log.info("The unique key is not unique as more than one vertex with the value {}", uniqueValue);
    } else {
      janusgraphVertex = graph.addVertex(faunusVertex.longId(),faunusVertex.label());
    }
    return janusgraphVertex;
}

def JanusGraphEdge getOrCreateEdge(FaunusEdge faunusEdge, JanusGraphVertex inVertex, JanusGraphVertex outVertex, JanusGraph graph, TaskInputOutputContext context, Logger log) {
    final String label = faunusEdge.label();

    log.debug("outVertex:{} label:{} inVertex:{}", outVertex, label, inVertex);

    final Edge janusgraphEdge = !outVertex.out(label).has("id", inVertex.id()).hasNext() ?
        graph.addEdge(null, outVertex, inVertex, label) :
        outVertex.outE(label).as("here").inV().has("id", inVertex.id()).back("here").next();

    return janusgraphEdge;
}

def void getOrCreateVertexProperty(JanusGraphProperty faunusProperty, JanusGraphVertex vertex, JanusGraph graph, TaskInputOutputContext context, Logger log) {

    final org.janusgraph.core.PropertyKey pkey = faunusProperty.propertyKey();
    if (pkey.cardinality().equals(org.janusgraph.core.Cardinality.SINGLE)) {
        vertex.property(pkey.name(), faunusProperty.value());
    } else {
//        Iterator<org.janusgraph.core.JanusGraphProperty> itty = vertex.getProperties(pkey.getName()).iterator();
//        if (!itty.hasNext()) {
            vertex.property(pkey.name(), faunusProperty.value());
//        }
    }

    log.debug("Set property {}={} on {}", pkey.name(), faunusProperty.value(), vertex);
}
