import com.thinkaurelius.titan.hadoop.FaunusVertex
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge
import com.thinkaurelius.titan.hadoop.FaunusVertex
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge
import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.Graph
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.apache.hadoop.mapreduce.Mapper

import static com.thinkaurelius.titan.hadoop.formats.BlueprintsGraphOutputMapReduce.Counters.*
import static com.thinkaurelius.titan.hadoop.formats.BlueprintsGraphOutputMapReduce.LOGGER

/**
 * This script is used to determine vertex and edge uniqueness within a pre-existing graph.
 * If the vertex/edge already exists in the graph, return it.
 * Else, if the vertex/edge does not already exist, create it and return it.
 * Any arbitrary function can be implemented. The two examples provided are typical scenarios.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
def Vertex getOrCreateVertex(final FaunusVertex hadoopVertex, final Graph graph, final Mapper.Context context) {
    final String uniqueKey = "name";
    final Object uniqueValue = hadoopVertex.getProperty(uniqueKey);
    final Vertex blueprintsVertex;
    if (null == uniqueValue)
        throw new RuntimeException("The provided Hadoop vertex does not have a property for the unique key: " + hadoopVertex);

    final Iterator<Vertex> itty = graph.query().has(uniqueKey, uniqueValue).vertices().iterator();
    if (itty.hasNext()) {
        blueprintsVertex = itty.next();
        context.getCounter(VERTICES_RETRIEVED).increment(1l);
        if (itty.hasNext()) {
            LOGGER.error("The unique key is not unique as more than one vertex with the value: " + uniqueValue);
        }
    } else {
        blueprintsVertex = graph.addVertex(hadoopVertex.getLongId());
        context.getCounter(VERTICES_WRITTEN).increment(1l);
    }

    // if vertex existed or not, add all the properties of the hadoopVertex to the blueprintsVertex
    for (final String property : hadoopVertex.getPropertyKeys()) {
        blueprintsVertex.setProperty(property, hadoopVertex.getProperty(property));
        context.getCounter(VERTEX_PROPERTIES_WRITTEN).increment(1l);
    }
    return blueprintsVertex;
}

def Edge getOrCreateEdge(final StandardFaunusEdge hadoopEdge, final Vertex blueprintsOutVertex, final Vertex blueprintsInVertex, final Graph graph, final Mapper.Context context) {
    final String edgeLabel = hadoopEdge.getLabel();
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

    // if edge existed or not, add all the properties of the hadoopEdge to the blueprintsEdge
    for (final String key : hadoopEdge.getPropertyKeys()) {
        blueprintsEdge.setProperty(key, hadoopEdge.getProperty(key));
        context.getCounter(EDGE_PROPERTIES_WRITTEN).increment(1l);
    }
    return blueprintsEdge;
}
