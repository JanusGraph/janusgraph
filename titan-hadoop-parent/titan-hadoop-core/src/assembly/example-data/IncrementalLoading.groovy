/*
 * This is the sample incremental loading script given in the manual.
 * 
 * See the documentation on Titan-Hadoop incremental loading and the 
 * config option "loader-script-file" for more information.
 */

def TitanVertex getOrCreateVertex(faunusVertex, graph, context, log) {
    String uniqueKey = "name";
    Object uniqueValue = faunusVertex.getProperty(uniqueKey);
    Vertex titanVertex;
    if (null == uniqueValue)
      throw new RuntimeException(faunusVertex + " has no value for key " + uniqueKey);

    Iterator<Vertex> itty = graph.query().has(uniqueKey, uniqueValue).vertices().iterator();
    if (itty.hasNext()) {
      titanVertex = itty.next();
      if (itty.hasNext())
        log.info("The key {} has duplicated value {}", uniqueKey, uniqueValue);
    } else {
      titanVertex = graph.addVertex(faunusVertex.getId());
    }
    return titanVertex;
}

def TitanEdge getOrCreateEdge(faunusEdge, inVertex, outVertex, graph, context, log) {
    final String label = faunusEdge.getLabel();

    log.debug("outVertex:{} label:{} inVertex:{}", outVertex, label, inVertex);

    final Edge titanEdge = !outVertex.out(label).has("id", inVertex.getId()).hasNext() ?
        graph.addEdge(null, outVertex, inVertex, label) :
        outVertex.outE(label).as("here").inV().has("id", inVertex.getId()).back("here").next();

    return titanEdge;
}

def Object getOrCreateVertexProperty(faunusProperty, vertex, graph, context, log) {

    final com.thinkaurelius.titan.core.PropertyKey pkey = faunusProperty.getPropertyKey();
    if (pkey.getCardinality().equals(com.thinkaurelius.titan.core.Cardinality.SINGLE)) {
        vertex.setProperty(pkey.getName(), faunusProperty.getValue());
    } else {
        vertex.addProperty(pkey.getName(), faunusProperty.getValue());
    }

    log.debug("Set property {}={} on {}", pkey.getName(), faunusProperty.getValue(), vertex);

    return faunusProperty.getValue();
}
