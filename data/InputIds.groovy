import com.thinkaurelius.faunus.FaunusVertex
import com.tinkerpop.blueprints.Direction

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

def void read(FaunusVertex vertex, String line) {
    parts = line.split(':');
    vertex.reuse(Long.valueOf(parts[0]))
    if (parts.length == 2) {
        parts[1].split(',').each {
            vertex.addEdge(Direction.OUT, 'linkedTo', Long.valueOf(it));
        }
    }
}