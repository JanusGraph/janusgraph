import com.thinkaurelius.faunus.FaunusEdge
import com.thinkaurelius.faunus.FaunusVertex
import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.Graph
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.apache.hadoop.mapreduce.Mapper

import static com.thinkaurelius.faunus.formats.BlueprintsGraphOutputMapReduce.Counters.*
import static com.thinkaurelius.faunus.formats.BlueprintsGraphOutputMapReduce.LOGGER

/**
 * This script is used to determine vertex and edge uniqueness within a pre-existing graph.
 * If the vertex/edge already exists in the graph, return it.
 * Else, if the vertex/edge does not already exist, create it and return it.
 * Any arbitrary function can be implemented. The two examples provided are typical scenarios.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
def Vertex getOrCreateVertex(final FaunusVertex faunusVertex, final Graph graph, final Mapper.Context context) {
    final String uniqueKey = "name";
    final Object uniqueValue = faunusVertex.getProperty(uniqueKey);
    final Vertex blueprintsVertex;
    if (null == uniqueValue)
        throw new RuntimeException("The provided Faunus vertex does not have a property for the unique key: " + faunusVertex);

    final Iterator<Vertex> itty = graph.query().has(uniqueKey, uniqueValue).vertices().iterator();
    if (itty.hasNext()) {
        blueprintsVertex = itty.next();
        context.getCounter(VERTICES_RETRIEVED).increment(1l);
        if (itty.hasNext()) {
            LOGGER.error("The unique key is not unique as more than one vertex with the value: " + uniqueValue);
        }
    } else {
        blueprintsVertex = graph.addVertex(faunusVertex.getIdAsLong());
        context.getCounter(VERTICES_WRITTEN).increment(1l);
    }

    // if vertex existed or not, add all the properties of the faunusVertex to the blueprintsVertex
    for (final String property : faunusVertex.getPropertyKeys()) {
        blueprintsVertex.setProperty(property, faunusVertex.getProperty(property));
        context.getCounter(VERTEX_PROPERTIES_WRITTEN).increment(1l);
    }
    return blueprintsVertex;
}

def Edge getOrCreateEdge(final FaunusEdge faunusEdge, final Vertex blueprintsOutVertex, final Vertex blueprintsInVertex, final Graph graph, final Mapper.Context context) {
    final String edgeLabel = faunusEdge.getLabel();
    final GremlinPipeline blueprintsEdgePipe = blueprintsOutVertex.outE(edgeLabel).as("e").inV().retain([blueprintsInVertex]).range(0, 1).back("e")
    final Edge blueprintsEdge;

    if (blueprintsEdgePipe.hasNext()) {
        blueprintsEdge = blueprintsEdgePipe.next();
        if (blueprintsEdgePipe.hasNext()) {
            LOGGER.error("There's more than one edge labeled '" + edgeLabel + "' between vertex #" + blueprintsOutVertex.getId() + " and vertex #" + blueprintsInVertex.getId());
        }
    } else {
        blueprintsEdge = graph.addEdge(null, blueprintsOutVertex, blueprintsInVertex, edgeLabel);
        context.getCounter(EDGES_WRITTEN).increment(1l);
    }

    // if edge existed or not, add all the properties of the faunusEdge to the blueprintsEdge
    for (final String key : faunusEdge.getPropertyKeys()) {
        blueprintsEdge.setProperty(key, faunusEdge.getProperty(key));
        context.getCounter(EDGE_PROPERTIES_WRITTEN).increment(1l);
    }
    return blueprintsEdge;
}