package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;
import com.thinkaurelius.titan.graphdb.olap.computer.FulgoraGraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.engine.ComputerTraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;

import java.util.stream.Stream;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractTitanGraphComputerProvider extends AbstractTitanGraphProvider {

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        return GraphTraversalSource.build().engine(ComputerTraversalEngine.build().computer(FulgoraGraphComputer.class)).create(graph);
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph, final TraversalStrategy... strategies) {
        final GraphTraversalSource.Builder builder = GraphTraversalSource.build().engine(ComputerTraversalEngine.build().computer(FulgoraGraphComputer.class));
        Stream.of(strategies).forEach(builder::with);
        return builder.create(graph);
    }

    @Override
    public ModifiableConfiguration getTitanConfiguration(String graphName, Class<?> test, String testMethodName) {
        return GraphDatabaseConfiguration.buildGraphConfiguration()
                .set(GraphDatabaseConfiguration.IDS_BLOCK_SIZE,1)
                .set(SimpleBulkPlacementStrategy.CONCURRENT_PARTITIONS,1)
                .set(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS, 2)
                .set(GraphDatabaseConfiguration.IDAUTHORITY_CAV_BITS,0);
    }

}
