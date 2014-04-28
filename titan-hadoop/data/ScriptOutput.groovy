import com.thinkaurelius.faunus.FaunusVertex
import com.tinkerpop.blueprints.Edge

import static com.tinkerpop.blueprints.Direction.IN
import static com.tinkerpop.blueprints.Direction.OUT

/**
 * An example Gremlin/Groovy script that writes a FaunusVertex to a text file line of the form:
 *   1:2,3,4,5
 * Vertex with id 1 links to vertices 2, 3, 4, and 5 (an adjacency list).
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

def void write(FaunusVertex vertex, DataOutput output) {
    output.writeUTF(vertex.getId().toString() + ':');
    Iterator<Edge> itty = vertex.getEdges(OUT).iterator()
    while (itty.hasNext()) {
        output.writeUTF(itty.next().getVertex(IN).getId().toString());
        if (itty.hasNext())
            output.writeUTF(',');
    }
    output.writeUTF('\n');
}