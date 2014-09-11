def TitanVertex getOrCreateVertex(faunusVertex, graph, context, log) {
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

def void getOrCreateVertexProperty(faunusProperty, vertex, graph, context, log) {

    final com.thinkaurelius.titan.core.PropertyKey pkey = faunusProperty.getPropertyKey();
    if (pkey.getCardinality().equals(com.thinkaurelius.titan.core.Cardinality.SINGLE)) {
        vertex.setProperty(pkey.getName(), faunusProperty.getValue());
    } else {
        Iterator<com.thinkaurelius.titan.core.TitanProperty> itty = vertex.getProperties(pkey.getName()).iterator();
        if (!itty.hasNext()) {
            vertex.addProperty(pkey.getName(), faunusProperty.getValue());
        }
    }

    log.debug("Set property {}={} on {}", pkey.getName(), faunusProperty.getValue(), vertex);
}
