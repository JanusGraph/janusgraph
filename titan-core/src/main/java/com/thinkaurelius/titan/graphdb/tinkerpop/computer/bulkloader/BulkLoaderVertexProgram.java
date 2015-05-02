package com.thinkaurelius.titan.graphdb.tinkerpop.computer.bulkloader;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.io.FileInputStream;
import java.util.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BulkLoaderVertexProgram implements VertexProgram<long[]> {

    // TODO: Be sure to accont for hidden properties --- though we may be changing the TP3 API soon for this.

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkLoaderVertexProgram.class);

    // TODO this is a step backwards in config management
    private static final String CFG_GRAPH_PREFIX = "titan.bulkload.graphconfig.";
    private static final String CFG_SCHEMA_CHECK = "titan.bulkload.schemacheck";
    private static final boolean CFG_SCHEMA_CHECK_DEFAULT = false;

    private static final String TITAN_ID = "titan.id";
    private static final ImmutableSet<String> elementComputeKeys = ImmutableSet.of(TITAN_ID);

    private MessageScope messageScope = MessageScope.Local.of(__::inE);
    private Configuration configuration;
    private TitanGraph graph;
    private TitanManagement mgmt;
    private boolean checkSchema;

    private BulkLoaderVertexProgram() {

    }

    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);
        this.configuration.getKeys().forEachRemaining(key -> {
            configuration.setProperty(CFG_GRAPH_PREFIX + key, this.configuration.getProperty(key));
        });
        configuration.setProperty(CFG_SCHEMA_CHECK, checkSchema);
    }


    public void loadState(final Configuration configuration) {
        this.configuration = new BaseConfiguration();
        configuration.getKeys().forEachRemaining(key -> {
            if (key.startsWith(CFG_GRAPH_PREFIX))
                this.configuration.setProperty(key.substring(CFG_GRAPH_PREFIX.length()), configuration.getProperty(key));
        });
        checkSchema = configuration.getBoolean(CFG_SCHEMA_CHECK, CFG_SCHEMA_CHECK_DEFAULT);
    }

    @Override
    public void setup(final Memory memory) {

    }

    @Override
    public void workerIterationStart(final Memory memory) {
        if (null == graph) {
            Preconditions.checkState(null == mgmt);
            graph = TitanFactory.open(configuration);
            LOGGER.info("Opened TitanGraph instance: {}", graph);
            mgmt = graph.openManagement();
            LOGGER.info("Opened TitanManagement instance: {}", mgmt);
        } else {
            LOGGER.warn("Leaked TitanGraph/TitanManagement instance(s): {}/{}", graph, mgmt);
        }
    }

    @Override
    public void workerIterationEnd(final Memory memory) {
        if (null != mgmt) {
            LOGGER.info("Rolling back management system: {}", mgmt);
            mgmt.rollback();
            LOGGER.info("Rolled back management system: {}", mgmt);
            mgmt = null;
        }

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
    public void execute(final Vertex vertex, final Messenger<long[]> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            // create the vertex in titan
            final Vertex titanVertex = graph.addVertex(T.label, vertex.label());
            // enforce property constraints if so configured
            if (checkSchema) {
                LOGGER.debug("Checking properties on vertex {}", vertex);
                checkProperties(vertex);
            }
            // write all the properties of the vertex to the newly created titan vertex
            vertex.properties().forEachRemaining(vertexProperty -> {
                // Set properties
                VertexProperty titanVertexProperty = titanVertex.<Object>property(vertexProperty.key(), vertexProperty.value());
                // Set properties on properties (metaproperties)
                vertexProperty.properties().forEachRemaining(metaProperty -> titanVertexProperty.<Object>property(metaProperty.key(), metaProperty.value()));
            });
            //LOGGER.info("Committing a transaction in vertex writing: " + vertex);
            // vertex.properties().remove();  TODO: optimization to drop data that is not needed in second iteration
            // set a dummy property that is the titan id of this particular vertex
            vertex.property(TITAN_ID, titanVertex.id());
            // create an id/titan_id pair and send it to all the vertex's outgoing adjacent vertices
            final long[] idPair = {Long.valueOf(vertex.id().toString()), (Long) titanVertex.id()};
            messenger.sendMessage(this.messageScope, idPair);
        } else {
            // create a id/titan_id map and populate it with all the incoming messages
            final Map<Long, Long> idPairs = new HashMap<>();
            messenger.receiveMessages(this.messageScope).forEachRemaining(idPair -> idPairs.put(idPair[0], idPair[1]));
            // get the titan vertex out of titan given the dummy id property
            Object vid = vertex.value(TITAN_ID);
            final Vertex titanVertex = graph.vertices(vid).next();
            // enforce edge constraints if so configured
            if (checkSchema) {
                LOGGER.debug("Checking edges on vertex {}", vertex);
                checkEdges(vertex);
            }
            // for all the incoming edges of the vertex, get the incoming adjacent vertex and write the edge and its properties
            vertex.edges(Direction.OUT).forEachRemaining(edge -> {
                final Vertex outgoingAdjacent = graph.vertices(idPairs.get(Long.valueOf(edge.inVertex().id().toString()))).next();
                final Edge titanEdge = titanVertex.addEdge(edge.label(), outgoingAdjacent);
                edge.properties().forEachRemaining(property -> titanEdge.<Object>property(property.key(), property.value()));
            });

            // vertex.bothE().remove(); TODO: optimization to drop data that is not needed any longer
            // vertex.properties().remove(); TODO: optimization to drop data that is not needed any longer
            //LOGGER.info("Committing a transaction in edge writing: " + vertex);
        }
    }

    private void checkProperties(Vertex vertex) {
        vertex.properties().forEachRemaining(vertexProperty -> {
            PropertyKey pk = mgmt.getPropertyKey(vertexProperty.label());
            if (null == pk) {
                unknownPropertyKey(vertex, vertexProperty.label());
            }

            Object o = vertexProperty.value();

            if (null == o) {
                LOGGER.debug("Skipping key {} (null value)", vertexProperty.label());
                return;
            }

            LOGGER.debug("Checking property {}", vertexProperty);

            Cardinality card = pk.cardinality();

            if (Cardinality.SINGLE == card && List.class.isAssignableFrom(o.getClass())) {
                // List-valuedness is probably sufficient to throw an exception, but we'll allow a singleton here
                List<?> l = (List<?>)o;
                int size = l.size();
                if (1 < size) {
                    multipleValuesViolatesCardinalitySingle(vertex, vertexProperty.label(), l);
                }
            } else if (Cardinality.SET == card && Collection.class.isAssignableFrom(o.getClass())) {
                // Check for duplicate values
                Collection<Object> c = (Collection<Object>)o;
                Set<Object> checkSet = new HashSet<>(c.size());
                for (Object elem : c) {
                    if (checkSet.contains(elem)) {
                        multipleValuesViolatesCardinalitySet(vertex, vertexProperty.label(), elem);
                    }
                    checkSet.add(elem);
                }
            }
        });
    }

    private void checkEdges(Vertex vertex) {

        Iterator<Edge> inEdges = vertex.edges(Direction.IN);

        Set<String> simpleLabels = new HashSet<>();

        while (inEdges.hasNext()) {
            Edge e = inEdges.next();
            String edgeLabelName = e.label();
            EdgeLabel edgeLabel = mgmt.getEdgeLabel(edgeLabelName);

            if (null == edgeLabel)
                unknownEdgeLabel(vertex, edgeLabelName);

            Multiplicity multi = edgeLabel.multiplicity();
            if (multi == Multiplicity.ONE2MANY) {
                long count = Iterators.size(vertex.edges(Direction.IN, edgeLabelName));
                if (1 < count) {
                    multipleEdgesViolatesOne2Many(vertex, edgeLabelName);
                }
            }

            if (multi == Multiplicity.SIMPLE)
                simpleLabels.add(edgeLabelName);
        }

        Iterator<Edge> outEdges = vertex.edges(Direction.OUT);

        while (outEdges.hasNext()) {
            Edge e = outEdges.next();
            String edgeLabelName = e.label();
            EdgeLabel edgeLabel = mgmt.getEdgeLabel(edgeLabelName);
            Multiplicity multi = edgeLabel.multiplicity();
            if (multi == Multiplicity.MANY2ONE) {
                //long count = vertex.outE(edgeLabelName).count().next().longValue();
                long count = Iterators.size(vertex.edges(Direction.OUT, edgeLabelName));
                if (1 < count) {
                    multipleEdgesViolateMany2One(vertex, edgeLabelName);
                }
            }

            if (multi == Multiplicity.SIMPLE)
                simpleLabels.add(edgeLabelName);
        }

        for (String simpleLabel : simpleLabels) {
            Iterator<Edge> bothEdges = vertex.edges(Direction.BOTH, simpleLabel);

            HashMap<Vertex, Vertex> simpleMap = new HashMap<>();

            while (bothEdges.hasNext()) {
                Edge e = bothEdges.next();
                Vertex outV = e.outVertex();
                Vertex inV = e.inVertex();

                Vertex otherV = outV.equals(vertex) ? inV : outV;

                if (otherV.equals(simpleMap.get(vertex)))
                    multipleEdgesViolateSimple(vertex, simpleLabel);

                simpleMap.put(vertex, otherV);
            }
        }

        // TODO edge props?
    }

    private void unknownPropertyKey(Vertex v, String key) {
        throw new IllegalArgumentException(String.format("Vertex %s has unknown property key %s", v, key));
    }

    private void unknownEdgeLabel(Vertex v, String label) {
        throw new IllegalArgumentException(String.format("Vertex %s has an incident edge with unknown label %s", v, label));
    }

    private void multipleValuesViolatesCardinalitySingle(Vertex v, String key, List<?> valueList) {
        String msg = String.format(
                "Vertex %s's property key %s with value list %s violates cardinality constraint %s",
                v, key, valueList, Cardinality.SINGLE);
        throw new IllegalArgumentException(msg);
    }

    private void multipleValuesViolatesCardinalitySet(Vertex v, String key, Object duplicateElem) {
        String msg = String.format(
                "Vertex %s's property key %s violates %s constraint: multiple occurrences of value %s found",
                v, key, Cardinality.SET, duplicateElem);
        throw new IllegalArgumentException(msg);
    }

    private void multipleEdgesViolateSimple(Vertex vertex, String simpleLabel) {
        String msg = String.format(
                "Vertex %s has multiple edges with label %s; this violates the label's multiplicity %s",
                vertex, simpleLabel, Multiplicity.SIMPLE);
        throw new IllegalArgumentException(msg);
    }

    private void multipleEdgesViolateMany2One(Vertex vertex, String edgeLabelName) {
        String msg = String.format(
                "Vertex %s has multiple edges with label %s that violate the label's multiplicity constraint %s",
                vertex, edgeLabelName, Multiplicity.MANY2ONE);
        throw new IllegalArgumentException(msg);
    }

    private void multipleEdgesViolatesOne2Many(Vertex vertex, String edgeLabelName) {
        String msg = String.format(
                "Vertex %s has multiple edges with label %s that violate the label's multiplicity constraint %s",
                vertex, edgeLabelName, Multiplicity.ONE2MANY);
        throw new IllegalArgumentException(msg);
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
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return ImmutableSet.of(messageScope);
    }

    @Override
    public VertexProgram<long[]> clone() {
        return this;
    }

    // TODO verify ResultGraph.ORIGINAL & Persist.EDGES trigger intended behavior

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.ORIGINAL;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.EDGES;
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
                properties.forEach((key, value) -> this.configuration.setProperty(CFG_GRAPH_PREFIX + key, value));
                return this;
            } catch (final Exception e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }

    }
}

