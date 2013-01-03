package com.thinkaurelius.faunus.mapreduce.blueprints;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.formats.titan.cassandra.TitanCassandraOutputFormat;
import com.thinkaurelius.faunus.formats.titan.hbase.TitanHBaseOutputFormat;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
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

    private static boolean commitGraph(final Graph graph, final long mutations) {
        if (mutations % MUTATION_COMMITS == 0) {
            if (graph instanceof TransactionalGraph) {
                ((TransactionalGraph) graph).stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
                return true;
            }
        }
        return false;
    }

    private static Graph generateGraph(final Configuration config) {
        final Class<? extends OutputFormat> format = config.getClass(FaunusGraph.GRAPH_OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class);
        if (TitanCassandraOutputFormat.class.isAssignableFrom(format)) {
            final BaseConfiguration titanconfig = new BaseConfiguration();
            titanconfig.setProperty("autotype", "blueprints");
            titanconfig.setProperty("storage.backend", "cassandra");
            titanconfig.setProperty("storage.hostname", ConfigHelper.getOutputInitialAddress(config));
            titanconfig.setProperty("storage.keyspace", ConfigHelper.getOutputKeyspace(config));
            titanconfig.setProperty("storage.port", ConfigHelper.getOutputRpcPort(config));
            return TitanFactory.open(titanconfig);
        } else if (TitanHBaseOutputFormat.class.isAssignableFrom(format)) {
            final BaseConfiguration titanconfig = new BaseConfiguration();
            titanconfig.setProperty("autotype", "blueprints");
            titanconfig.setProperty("storage.backend", "hbase");
            titanconfig.setProperty("storage.tablename", config.get(TableOutputFormat.OUTPUT_TABLE));
            titanconfig.setProperty("storage.hostname", config.get(HBaseStoreManager.HBASE_CONFIGURATION_MAP.get(GraphDatabaseConfiguration.HOSTNAME_KEY)));
            if (config.get(HBaseStoreManager.HBASE_CONFIGURATION_MAP.get(GraphDatabaseConfiguration.PORT_KEY), null) != null)
                titanconfig.setProperty("storage.port", config.get(HBaseStoreManager.HBASE_CONFIGURATION_MAP.get(GraphDatabaseConfiguration.PORT_KEY)));
            return TitanFactory.open(titanconfig);
        } else {
            // TODO: this is where Rexster can come into play here
            throw new RuntimeException("The provide graph output format is not supported: " + format.getName());
        }
    }

    ////////////// MAP/REDUCE WORK FROM HERE ON OUT

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>> {

        private Graph graph;
        long counter = 0;

        private final Holder<FaunusVertex> vertexHolder = new Holder<FaunusVertex>();
        private final LongWritable longWritable = new LongWritable();
        private final FaunusVertex shellVertex = new FaunusVertex();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.graph = WriteGraphMapReduce.generateGraph(context.getConfiguration());
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) throws IOException, InterruptedException {

            // Write FaunusVertex (and respective properties) to Blueprints Graph
            final Vertex titanVertex = this.graph.addVertex(null);
            context.getCounter(Counters.VERTICES_WRITTEN).increment(1l);
            for (final String property : value.getPropertyKeys()) {
                titanVertex.setProperty(property, value.getProperty(property));
                context.getCounter(Counters.VERTEX_PROPERTIES_WRITTEN).increment(1l);
            }
            value.setProperty(BLUEPRINTS_ID, titanVertex.getId());

            // Propagate shell vertices with Blueprints ids
            this.shellVertex.reuse(value.getIdAsLong());
            this.shellVertex.setProperty(BLUEPRINTS_ID, titanVertex.getId());
            // TODO: Might need to be OUT for the sake of unidirectional edges in Titan
            for (final Edge faunusEdge : value.getEdges(IN)) {
                this.longWritable.set((Long) faunusEdge.getVertex(OUT).getId());
                context.write(this.longWritable, this.vertexHolder.set('s', this.shellVertex));
            }

            this.longWritable.set(value.getIdAsLong());
            context.write(this.longWritable, this.vertexHolder.set('v', value));

            // after so many mutations, successfully commit the transaction (if graph is transactional)
            WriteGraphMapReduce.commitGraph(this.graph, ++this.counter);
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
            this.graph = WriteGraphMapReduce.generateGraph(context.getConfiguration());
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder<FaunusVertex>> values, final Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex faunusVertex = new FaunusVertex();
            // generate a map of the faunus id with the blueprints id for all shell vertices (vertices incoming adjacent)
            final java.util.Map<Long, Long> faunusTitanIdMap = new HashMap<Long, Long>();
            for (final Holder<FaunusVertex> holder : values) {
                if (holder.getTag() == 's') {
                    faunusTitanIdMap.put(holder.get().getIdAsLong(), (Long) holder.get().getProperty(BLUEPRINTS_ID));
                } else {
                    faunusVertex.addAll(holder.get());
                }
            }

            Vertex titanVertex = this.graph.getVertex(faunusVertex.getProperty(BLUEPRINTS_ID));
            for (final Edge faunusEdge : faunusVertex.getEdges(OUT)) {
                final Edge titanEdge = this.graph.addEdge(null, titanVertex, this.graph.getVertex(faunusTitanIdMap.get((Long) faunusEdge.getVertex(IN).getId())), faunusEdge.getLabel());
                context.getCounter(Counters.EDGES_WRITTEN).increment(1l);
                for (final String property : faunusEdge.getPropertyKeys()) {
                    titanEdge.setProperty(property, faunusEdge.getProperty(property));
                    context.getCounter(Counters.EDGE_PROPERTIES_WRITTEN).increment(1l);
                }
                // after so many mutations, successfully commit the transaction (if graph is transactional)
                // for titan, if the transaction is committed, need to 'reget' the vertex
                if (WriteGraphMapReduce.commitGraph(this.graph, ++this.counter))
                    titanVertex = this.graph.getVertex(titanVertex.getId());
            }

            // this is a sideEffect, thus remove the created blueprints id property
            faunusVertex.removeProperty(BLUEPRINTS_ID);
            context.write(NullWritable.get(), faunusVertex);
        }

        @Override
        public void cleanup(final Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (this.graph instanceof TransactionalGraph)
                ((TransactionalGraph) this.graph).stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        }
    }
}
