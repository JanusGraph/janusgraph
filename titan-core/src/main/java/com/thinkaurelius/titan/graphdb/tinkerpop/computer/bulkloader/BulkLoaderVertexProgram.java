package com.thinkaurelius.titan.graphdb.tinkerpop.computer.bulkloader;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BulkLoaderVertexProgram implements VertexProgram<Long[]> {

    private static final String TITAN_CONFIGURATION_LOCATION = "titan.configuration.location";
    private static final String TITAN_ID = Graph.Key.hide("titan.id");
    private static final Set<String> elementComputeKeys = new HashSet<>();

    static {
        elementComputeKeys.add(TITAN_ID);
    }

    private MessageType.Local messageType = MessageType.Local.of(() -> GraphTraversal.<Vertex>of().outE());
    private TitanGraph graph;
    private String location;

    private BulkLoaderVertexProgram() {

    }

    public void storeState(final Configuration configuration) {
        configuration.setProperty(TITAN_CONFIGURATION_LOCATION, this.location);
    }


    public void loadState(final Configuration configuration) {
        this.location = configuration.getString(TITAN_CONFIGURATION_LOCATION);
        this.graph = TitanFactory.open(this.location);
    }

    @Override
    public void setup(final Memory memory) {

    }

    @Override
    public void execute(final Vertex vertex, final Messenger<Long[]> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            // create the vertex in titan
            final Vertex titanVertex = this.graph.addVertex(T.label, vertex.label());
            // write all the properties of the vertex to the newly created titan vertex
            vertex.properties().forEachRemaining(vertexProperty -> titanVertex.<Object>property(vertexProperty.key(), vertexProperty.value()));
            this.graph.tx().commit();
            // vertex.properties().remove();  TODO: optimization to drop data that is not needed in second iteration
            // set a dummy property that is the titan id of this particular vertex
            vertex.property(TITAN_ID, titanVertex.id());
            // create an id/titan_id pair and send it to all the vertex's outgoing adjacent vertices
            final Long[] idPair = {Long.valueOf(vertex.id().toString()), (Long) titanVertex.id()};
            messenger.sendMessage(this.messageType, idPair);
        } else {
            // create a id/titan_id map and populate it with all the incoming messages
            final Map<Long, Long> idPairs = new HashMap<>();
            messenger.receiveMessages(this.messageType).forEach(idPair -> idPairs.put(idPair[0], idPair[1]));
            // get the titan vertex out of titan given the dummy id property
            final Vertex titanVertex = this.graph.v(vertex.value(TITAN_ID));
            // for all the incoming edges of the vertex, get the incoming adjacent vertex and write the edge and its properties
            vertex.inE().forEachRemaining(edge -> {
                final Vertex incomingAdjacent = this.graph.v(idPairs.get(Long.valueOf(edge.outV().id().next().toString())));
                final Edge titanEdge = incomingAdjacent.addEdge(edge.label(), titanVertex);
                edge.properties().forEachRemaining(property -> titanEdge.<Object>property(property.key(), property.value()));
            });
            // vertex.bothE().remove(); TODO: optimization to drop data that is not needed any longer
            // vertex.properties().remove(); TODO: optimization to drop data that is not needed any longer
            this.graph.tx().commit();
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        return memory.getIteration() >= 1;
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return elementComputeKeys;
    }

    ////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {

        private Builder() {
            super(BulkLoaderVertexProgram.class);
        }

        public Builder titan(final String location) {
            this.configuration.setProperty(TITAN_CONFIGURATION_LOCATION, location);
            return this;
        }

    }
}

