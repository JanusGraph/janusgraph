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


import com.tinkerpop.blueprints.Edge
import org.janusgraph.hadoop.FaunusVertex

import static com.tinkerpop.blueprints.Direction.IN
import static com.tinkerpop.blueprints.Direction.OUT

void write(FaunusVertex vertex, DataOutput output) {
    output.writeUTF(vertex.id().toString() + ':')
    Iterator<Edge> itty = vertex.getEdges(OUT).iterator()
    while (itty.hasNext()) {
        output.writeUTF(itty.next().getVertex(IN).getId().toString())
        if (itty.hasNext()) {
            output.writeUTF(',')
        }
    }
    output.writeUTF('\n')
}
