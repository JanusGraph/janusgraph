/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

def parse(line) {
    def (vertex, outEdges, inEdges) = line.split(/\t/, 3)
    def (v1id, v1label, v1props) = vertex.split(/,/, 3)
    def v1 = graph.addVertex(T.id, v1id.toInteger(), T.label, v1label)
    switch (v1label) {
        case "song":
            def (name, songType, performances) = v1props.split(/,/)
            v1.property("name", name)
            v1.property("songType", songType)
            v1.property("performances", performances.toInteger())
            break
        case "artist":
            v1.property("name", v1props)
            break
        default:
            throw new Exception("Unexpected vertex label: ${v1label}")
    }
    [[outEdges, true], [inEdges, false]].each { def edges, def out ->
        edges.split(/\|/).grep().each { def edge ->
            def parts = edge.split(/,/)
            def otherV, eLabel, weight = null
            if (parts.size() == 2) {
                (eLabel, otherV) = parts
            } else {
                (eLabel, otherV, weight) = parts
            }
            def v2 = graph.addVertex(T.id, otherV.toInteger())
            def e = out ? v1.addOutEdge(eLabel, v2) : v1.addInEdge(eLabel, v2)
            if (weight != null) {
                e.property("weight", weight.toInteger())
            }
        }
    }
    return v1
}
