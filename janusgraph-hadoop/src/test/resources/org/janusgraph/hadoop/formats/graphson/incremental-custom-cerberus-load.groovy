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

import org.janusgraph.core.JanusGraphVertexProperty

JanusGraphVertex getOrCreateVertex(faunusVertex, graph, context, log) {
    String uniqueKey = "name"
    Object uniqueValue = faunusVertex.value(uniqueKey)
    Vertex janusgraphVertex
    if (null == uniqueValue) {
        throw new RuntimeException("The provided Faunus vertex does not have a property for the unique key: " + faunusVertex)
    }
    Iterator<Vertex> itty = graph.query().has(uniqueKey, uniqueValue).vertices().iterator()
    if (itty.hasNext()) {
      janusgraphVertex = itty.next()
      if (itty.hasNext()) {
          log.info("The unique key is not unique as more than one vertex with the value {}", uniqueValue)
      }
    } else {
      janusgraphVertex = graph.addVertex(faunusVertex.longId(),faunusVertex.label())
    }
    return janusgraphVertex
}

void getOrCreateVertexProperty(faunusProperty, vertex, graph, context, log) {

    final org.janusgraph.core.PropertyKey pkey = faunusProperty.propertyKey()
    if (pkey.cardinality().equals(org.janusgraph.core.Cardinality.SINGLE)) {
        vertex.property(pkey.name(), faunusProperty.value())
    } else {
        Iterator<JanusGraphVertexProperty> itty = vertex.getProperties(pkey.name()).iterator()
        if (!itty.hasNext()) {
            vertex.property(pkey.name(), faunusProperty.value())
        }
    }

    log.debug("Set property {}={} on {}", pkey.name(), faunusProperty.value(), vertex)
}
