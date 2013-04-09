import com.thinkaurelius.faunus.FaunusVertex
import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.Edge

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

def void write(FaunusVertex vertex, DataOutputStream out) {
    out.writeUTF(vertex.getId().toString() + ":");
    Iterator<Edge> itty = vertex.getEdges(Direction.OUT).iterator()
    while (itty.hasNext()) {
        out.writeUTF(itty.next().getId().toString());
        if (itty.hasNext())
            out.writeUTF(',');
    }
    out.writeUTF('\n');
}