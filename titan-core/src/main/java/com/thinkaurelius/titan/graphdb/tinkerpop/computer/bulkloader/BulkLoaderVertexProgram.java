package com.thinkaurelius.titan.graphdb.tinkerpop.computer.bulkloader;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.util.system.IOUtils;
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
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BulkLoaderVertexProgram implements VertexProgram<Long[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkLoaderVertexProgram.class);

    private static final String CONFIGURATION_PREFIX = "titan.bulkLoaderVertexProgram.";
    private static final String TITAN_ID = Graph.Key.hide("titan.id");
    private static final ImmutableSet<String> elementComputeKeys = ImmutableSet.of(TITAN_ID);

    private MessageType.Local messageType = MessageType.Local.of(() -> GraphTraversal.<Vertex>of().outE());
    private Configuration configuration;  // TODO: TitanGraph.configuration() needs to be implemented
    private TitanGraph graph;

    private BulkLoaderVertexProgram() {

    }

    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);
        this.configuration.getKeys().forEachRemaining(key -> {
            configuration.setProperty(CONFIGURATION_PREFIX + key, this.configuration.getProperty(key));
        });
    }


    public void loadState(final Configuration configuration) {
        this.configuration = new BaseConfiguration();
        configuration.getKeys().forEachRemaining(key -> {
            if (key.startsWith(CONFIGURATION_PREFIX))
                this.configuration.setProperty(key.substring(CONFIGURATION_PREFIX.length()), configuration.getProperty(key));
        });
    }

    @Override
    public void setup(final Memory memory) {

    }

    @Override
    public void workerIterationStart(final Memory memory) {
        if (null == graph) {
            graph = TitanFactory.open(configuration);
            LOGGER.info("Opened TitanGraph instance: {}", graph);
        } else {
            LOGGER.warn("Leaked TitanGraph instance: {}", graph);
        }
    }

    @Override
    public void workerIterationEnd(final Memory memory) {
        if (null != graph) {
            LOGGER.info("Committing transaction on TitanGraph instance: {}", graph);
            graph.tx().commit(); // TODO will Giraph/MR restart the program and re-run execute if this fails?
            LOGGER.debug("Committed transaction on TitanGraph instance: {}", graph);
            IOUtils.closeQuietly(graph);
            LOGGER.info("Closed TitanGraph instance: {}", graph);
            graph = null;
        }
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<Long[]> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            // create the vertex in titan
            final Vertex titanVertex = graph.addVertex(T.label, vertex.label());
            // write all the properties of the vertex to the newly created titan vertex
            vertex.properties().forEachRemaining(vertexProperty -> titanVertex.<Object>property(vertexProperty.key(), vertexProperty.value()));
            //LOGGER.info("Committing a transaction in vertex writing: " + vertex);
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
            final Vertex titanVertex = graph.v(vertex.value(TITAN_ID));
            // for all the incoming edges of the vertex, get the incoming adjacent vertex and write the edge and its properties
            vertex.inE().forEachRemaining(edge -> {
                final Vertex incomingAdjacent = graph.v(idPairs.get(Long.valueOf(edge.outV().id().next().toString())));
                final Edge titanEdge = incomingAdjacent.addEdge(edge.label(), titanVertex);
                edge.properties().forEachRemaining(property -> titanEdge.<Object>property(property.key(), property.value()));
            });
            // vertex.bothE().remove(); TODO: optimization to drop data that is not needed any longer
            // vertex.properties().remove(); TODO: optimization to drop data that is not needed any longer
            //LOGGER.info("Committing a transaction in edge writing: " + vertex);
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

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this, "");
    }

    ////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {

        private Builder() {
            super(BulkLoaderVertexProgram.class);
        }

        public Builder titan(final String propertiesFileLocation) {
            try {
                final Properties properties = new Properties();
                properties.load(new FileInputStream(propertiesFileLocation));
                properties.forEach((key, value) -> this.configuration.setProperty(CONFIGURATION_PREFIX + key, value));
                return this;
            } catch (final Exception e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }

    }
}

