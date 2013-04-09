import com.thinkaurelius.faunus.FaunusVertex
import com.tinkerpop.blueprints.Edge

import static com.tinkerpop.blueprints.Direction.OUT

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

def String write(FaunusVertex vertex) {
    StringBuilder builder = new StringBuilder();
    builder.append(vertex.getId().toString() + ":");
    Iterator<Edge> itty = vertex.getEdges(OUT).iterator()
    while (itty.hasNext()) {
        builder.append(itty.next().getId());
        if (itty.hasNext())
            builder.append(',');
    }
    return builder.toString();
}