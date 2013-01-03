package com.thinkaurelius.faunus.mapreduce.blueprints;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.titan.core.TitanFactory;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class WriteGraphMapReduce {

    public enum Counters {
        VERTICES_WRITTEN,
        VERTEX_PROPERTIES_WRITTEN,
        EDGES_WRITTEN,
        EDGE_PROPERTIES_WRITTEN
    }

    public static final String BLUEPRINTS_ID = "_blueprintsId";
    private static final long MUTATION_COMMITS = 5000;

    private static void commitGraph(final Graph graph, long mutations) {
        if (mutations % MUTATION_COMMITS == 0) {
            if (graph instanceof TransactionalGraph)
                ((TransactionalGraph) graph).stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        }
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>> {

        private Graph graph;
        long counter = 0;

        private final Holder<FaunusVertex> vertexHolder = new Holder<FaunusVertex>();
        private final LongWritable longWritable = new LongWritable();
        private final FaunusVertex shellVertex = new FaunusVertex();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            final BaseConfiguration titanconfig = new BaseConfiguration();
            titanconfig.setProperty("autotype", "blueprints");
            titanconfig.setProperty("storage.backend", "cassandra");
            titanconfig.setProperty("storage.hostname", ConfigHelper.getOutputInitialAddress(config));
            titanconfig.setProperty("storage.keyspace", ConfigHelper.getOutputKeyspace(config));
            titanconfig.setProperty("storage.port", ConfigHelper.getOutputRpcPort(config));
            this.graph = TitanFactory.open(titanconfig);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) throws IOException, InterruptedException {

            // Write FaunusVertex (and respective properties) to Titan
            final Vertex vertex = this.graph.addVertex(null);
            for (final String property : value.getPropertyKeys()) {
                vertex.setProperty(property, value.getProperty(property));
            }
            value.setProperty(BLUEPRINTS_ID, vertex.getId());

            // Propagate holders and ids
            for (Edge edge : value.getEdges(IN)) {
                this.longWritable.set((Long) edge.getVertex(OUT).getId());
                this.shellVertex.reuse(value.getIdAsLong());
                this.shellVertex.setProperty(BLUEPRINTS_ID, vertex.getId());
                context.write(this.longWritable, vertexHolder.set('s', this.shellVertex));
            }

            this.longWritable.set(value.getIdAsLong());
            context.write(this.longWritable, vertexHolder.set('v', value));

            WriteGraphMapReduce.commitGraph(this.graph, this.counter++);
        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) throws IOException, InterruptedException {
            if (this.graph instanceof TransactionalGraph)
                ((TransactionalGraph) this.graph).stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex> {

        private Graph graph;
        long counter = 0;

        @Override
        public void setup(final Reduce.Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            final BaseConfiguration titanconfig = new BaseConfiguration();
            titanconfig.setProperty("autotype", "blueprints");
            titanconfig.setProperty("storage.backend", "cassandra");
            titanconfig.setProperty("storage.hostname", ConfigHelper.getOutputInitialAddress(config));
            titanconfig.setProperty("storage.keyspace", ConfigHelper.getOutputKeyspace(config));
            titanconfig.setProperty("storage.port", ConfigHelper.getOutputRpcPort(config));
            this.graph = TitanFactory.open(titanconfig);
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder<FaunusVertex>> values, final Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex();
            final java.util.Map<Long, Long> faunusTitanIdMap = new HashMap<Long, Long>();
            for (final Holder<FaunusVertex> holder : values) {
                if (holder.getTag() == 's') {
                    faunusTitanIdMap.put(holder.get().getIdAsLong(), (Long) holder.get().getProperty(BLUEPRINTS_ID));
                } else {
                    vertex.addAll(holder.get());
                }
            }

            final Vertex root = this.graph.getVertex(vertex.getProperty(BLUEPRINTS_ID));
            for (final Edge edge : vertex.getEdges(OUT)) {
                final Edge e = this.graph.addEdge(null, root, this.graph.getVertex(faunusTitanIdMap.get((Long) edge.getVertex(IN).getId())), edge.getLabel());
                for (final String property : edge.getPropertyKeys()) {
                    e.setProperty(property, edge.getProperty(property));
                }
                WriteGraphMapReduce.commitGraph(this.graph, this.counter++);
            }

            vertex.removeProperty(BLUEPRINTS_ID);
            context.write(NullWritable.get(), vertex);
        }

        @Override
        public void cleanup(final Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (this.graph instanceof TransactionalGraph)
                ((TransactionalGraph) this.graph).stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        }
    }
}
