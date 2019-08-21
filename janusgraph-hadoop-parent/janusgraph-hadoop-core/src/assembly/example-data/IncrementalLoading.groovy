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

/*
 * This is the sample incremental loading script given in the manual.
 * 
 * See the documentation on JanusGraph-Hadoop incremental loading and the 
 * config option "loader-script-file" for more information.
 */

JanusGraphVertex getOrCreateVertex(faunusVertex, graph, context, log) {
    String uniqueKey = "name"
    Object uniqueValue = faunusVertex.getProperty(uniqueKey)
    Vertex janusgraphVertex
    if (null == uniqueValue) {
        throw new RuntimeException(faunusVertex + " has no value for key " + uniqueKey)
    }
    Iterator<Vertex> itty = graph.query().has(uniqueKey, uniqueValue).vertices().iterator()
    if (itty.hasNext()) {
      janusgraphVertex = itty.next()
      if (itty.hasNext()) {
          log.info("The key {} has duplicated value {}", uniqueKey, uniqueValue)
      }
    } else {
      janusgraphVertex = graph.addVertex(faunusVertex.getId())
    }
    return janusgraphVertex
}

JanusGraphEdge getOrCreateEdge(faunusEdge, inVertex, outVertex, graph, context, log) {
    final String label = faunusEdge.getLabel()

    log.debug("outVertex:{} label:{} inVertex:{}", outVertex, label, inVertex)

    final Edge janusgraphEdge = !outVertex.out(label).has("id", inVertex.getId()).hasNext() ?
        graph.addEdge(null, outVertex, inVertex, label) :
        outVertex.outE(label).as("here").inV().has("id", inVertex.getId()).back("here").next()

    return janusgraphEdge
}

Object getOrCreateVertexProperty(faunusProperty, vertex, graph, context, log) {

    final org.janusgraph.core.PropertyKey pkey = faunusProperty.getPropertyKey()
    if (pkey.cardinality().equals(org.janusgraph.core.Cardinality.SINGLE)) {
        vertex.setProperty(pkey.name(), faunusProperty.getValue())
    } else {
        vertex.addProperty(pkey.name(), faunusProperty.getValue())
    }

    log.debug("Set property {}={} on {}", pkey.name(), faunusProperty.getValue(), vertex)

    return faunusProperty.getValue()
}
