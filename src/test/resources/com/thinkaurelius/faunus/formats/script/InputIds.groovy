import com.thinkaurelius.faunus.FaunusVertex
import com.tinkerpop.blueprints.Direction

def boolean read(FaunusVertex v, String line) {
    parts = line.split(':');
    v.reuse(Long.valueOf(parts[0]))
    if (parts.length == 2) {
        parts[1].split(',').each {
            v.addEdge(Direction.OUT, 'linkedTo', Long.valueOf(it));
        }
    }
    return true;
}