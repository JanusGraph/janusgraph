import com.thinkaurelius.titan.core.TitanVertexProperty

def TitanVertex getOrCreateVertex(faunusVertex, graph, context, log) {
    String uniqueKey = "name";
    Object uniqueValue = faunusVertex.value(uniqueKey);
    Vertex titanVertex;
    if (null == uniqueValue)
      throw new RuntimeException("The provided Faunus vertex does not have a property for the unique key: " + faunusVertex);
  
    Iterator<Vertex> itty = graph.query().has(uniqueKey, uniqueValue).vertices().iterator();
    if (itty.hasNext()) {
      titanVertex = itty.next();
      if (itty.hasNext())
        log.info("The unique key is not unique as more than one vertex with the value {}", uniqueValue);
    } else {
      titanVertex = graph.addVertex(faunusVertex.longId(),faunusVertex.label());
    }
    return titanVertex;
}

def void getOrCreateVertexProperty(faunusProperty, vertex, graph, context, log) {

    final com.thinkaurelius.titan.core.PropertyKey pkey = faunusProperty.propertyKey();
    if (pkey.cardinality().equals(com.thinkaurelius.titan.core.Cardinality.SINGLE)) {
        vertex.property(pkey.name(), faunusProperty.value());
    } else {
        Iterator<TitanVertexProperty> itty = vertex.getProperties(pkey.name()).iterator();
        if (!itty.hasNext()) {
            vertex.property(pkey.name(), faunusProperty.value());
        }
    }

    log.debug("Set property {}={} on {}", pkey.name(), faunusProperty.value(), vertex);
}
