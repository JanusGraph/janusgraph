def TitanVertex getOrCreateVertex(FaunusVertex faunusVertex, TitanGraph graph, TaskInputOutputContext context, Logger log) {
    String uniqueKey = "name";
    Object uniqueValue = faunusVertex.getProperty(uniqueKey);
    Vertex titanVertex;
    if (null == uniqueValue)
      throw new RuntimeException("The provided Faunus vertex does not have a property for the unique key: " + faunusVertex);
  
    Iterator<Vertex> itty = graph.query().has(uniqueKey, uniqueValue).vertices().iterator();
    if (itty.hasNext()) {
      titanVertex = itty.next();
      if (itty.hasNext())
        log.info("The unique key is not unique as more than one vertex with the value {}", uniqueValue);
    } else {
      titanVertex = graph.addVertex(faunusVertex.getId());
    }
    return titanVertex;
}

def TitanEdge getOrCreateEdge(FaunusEdge faunusEdge, TitanVertex inVertex, TitanVertex outVertex, TitanGraph graph, TaskInputOutputContext context, Logger log) {
    final String label = faunusEdge.getLabel();

    log.debug("outVertex:{} label:{} inVertex:{}", outVertex, label, inVertex);

    final Edge titanEdge = !outVertex.out(label).has("id", inVertex.getId()).hasNext() ?
        graph.addEdge(null, outVertex, inVertex, label) :
        outVertex.outE(label).as("here").inV().has("id", inVertex.getId()).back("here").next();

    return titanEdge;
}

def void getOrCreateVertexProperty(TitanProperty faunusProperty, TitanVertex vertex, TitanGraph graph, TaskInputOutputContext context, Logger log) {

    final com.thinkaurelius.titan.core.PropertyKey pkey = faunusProperty.getPropertyKey();
    if (pkey.getCardinality().equals(com.thinkaurelius.titan.core.Cardinality.SINGLE)) {
        vertex.setProperty(pkey.getName(), faunusProperty.getValue());
    } else {
//        Iterator<com.thinkaurelius.titan.core.TitanProperty> itty = vertex.getProperties(pkey.getName()).iterator();
//        if (!itty.hasNext()) {
            vertex.addProperty(pkey.getName(), faunusProperty.getValue());
//        }
    }

    log.debug("Set property {}={} on {}", pkey.getName(), faunusProperty.getValue(), vertex);
}
