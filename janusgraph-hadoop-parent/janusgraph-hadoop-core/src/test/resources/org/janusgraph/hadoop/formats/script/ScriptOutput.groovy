import org.janusgraph.hadoop.FaunusVertex
import com.tinkerpop.blueprints.Edge

import static com.tinkerpop.blueprints.Direction.IN
import static com.tinkerpop.blueprints.Direction.OUT

def void write(FaunusVertex vertex, DataOutput output) {
    output.writeUTF(vertex.id().toString() + ':');
    Iterator<Edge> itty = vertex.getEdges(OUT).iterator()
    while (itty.hasNext()) {
        output.writeUTF(itty.next().getVertex(IN).getId().toString());
        if (itty.hasNext())
            output.writeUTF(',');
    }
    output.writeUTF('\n');
}
