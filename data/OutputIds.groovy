import com.thinkaurelius.faunus.FaunusVertex
import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.Edge

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

def void write(FaunusVertex vertex, PrintWriter out) {
    out.write(vertex.getId().toString() + ":");
    Iterator<Edge> itty = vertex.getEdges(Direction.OUT).iterator()
    while (itty.hasNext()) {
        out.write(itty.next().getId().toString());
        if (itty.hasNext())
            out.write(',');
    }
    out.write('\n');
}