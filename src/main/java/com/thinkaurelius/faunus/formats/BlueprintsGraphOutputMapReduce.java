package com.thinkaurelius.faunus.formats;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.formats.titan.cassandra.TitanCassandraOutputFormat;
import com.thinkaurelius.faunus.formats.titan.hbase.TitanHBaseOutputFormat;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BlueprintsGraphOutputMapReduce {

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

    public static Graph generateGraph(final Configuration config) {
        final Class<? extends OutputFormat> format = config.getClass(FaunusGraph.GRAPH_OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class);
        if (format.equals(TitanCassandraOutputFormat.class)) {
            return TitanCassandraOutputFormat.generateGraph(config);
        } else if (format.equals(TitanHBaseOutputFormat.class)) {
            return TitanHBaseOutputFormat.generateGraph(config);
        } else {
            // TODO: this is where Rexster can come into play here
            throw new RuntimeException("The provide graph output format is not supported: " + format.getName());
        }
    }

    ////////////// MAP/REDUCE WORK FROM HERE ON OUT

    // WRITE ALL THE VERTICES AND THEIR PROPERTIES
    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>> {

        Graph graph;
        private long mutations = 0;

        private final Holder<FaunusVertex> vertexHolder = new Holder<FaunusVertex>();
        private final LongWritable longWritable = new LongWritable();
        private final FaunusVertex shellVertex = new FaunusVertex();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.graph = BlueprintsGraphOutputMapReduce.generateGraph(context.getConfiguration());
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) throws IOException, InterruptedException {

            // Write FaunusVertex (and respective properties) to Blueprints Graph
            final Vertex blueprintsVertex = this.graph.addVertex(null);
            context.getCounter(Counters.VERTICES_WRITTEN).increment(1l);
            for (final String property : value.getPropertyKeys()) {
                blueprintsVertex.setProperty(property, value.getProperty(property));
                context.getCounter(Counters.VERTEX_PROPERTIES_WRITTEN).increment(1l);
            }
            value.setProperty(BLUEPRINTS_ID, blueprintsVertex.getId());

            // Propagate shell vertices with Blueprints ids
            this.shellVertex.reuse(value.getIdAsLong());
            this.shellVertex.setProperty(BLUEPRINTS_ID, blueprintsVertex.getId());
            // TODO: Might need to be OUT for the sake of unidirectional edges in Titan
            for (final Edge faunusEdge : value.getEdges(IN)) {
                this.longWritable.set((Long) faunusEdge.getVertex(OUT).getId());
                context.write(this.longWritable, this.vertexHolder.set('s', this.shellVertex));
            }

            this.longWritable.set(value.getIdAsLong());
            context.write(this.longWritable, this.vertexHolder.set('v', value));

            // after so many mutations, successfully commit the transaction (if graph is transactional)
            BlueprintsGraphOutputMapReduce.commitGraph(this.graph, ++this.mutations);
        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) throws IOException, InterruptedException {
            if (this.graph instanceof TransactionalGraph)
                ((TransactionalGraph) this.graph).stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        }
    }

    // REDUCE THE AMOUNT OF TRAFFIC TO REDUCES BY FILTERING OUT DUPLICATE SHELL VERTICES
    public static class Combiner extends Reducer<LongWritable, Holder<FaunusVertex>, LongWritable, Holder<FaunusVertex>> {

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder<FaunusVertex>> values, final Reducer<LongWritable, Holder<FaunusVertex>, LongWritable, Holder<FaunusVertex>>.Context context) throws IOException, InterruptedException {
            // TODO: this should be a bloom filter.
            final Set seenBefore = new HashSet();
            for (final Holder<FaunusVertex> holder : values) {
                if (holder.getTag() == 's') {
                    final Object id = holder.get().getId();
                    if (!seenBefore.contains(id)) {
                        seenBefore.add(id);
                        context.write(key, holder);
                    }
                } else {
                    context.write(key, holder);
                }
            }
        }
    }

    // WRITE ALL THE EDGES CONNECTING THE VERTICES
    // TODO: If we can safely assume this is always going to be an OutputFormat then make it NullWritable/NullWritable (save memory)
    // TODO: ...or simply not keep around faunusVertex in the reduce() method and output a shell vertex.
    public static class Reduce extends Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex> {

        Graph graph;
        private long mutations = 0;

        @Override
        public void setup(final Reduce.Context context) throws IOException, InterruptedException {
            this.graph = BlueprintsGraphOutputMapReduce.generateGraph(context.getConfiguration());
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder<FaunusVertex>> values, final Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex faunusVertex = new FaunusVertex();
            // generate a map of the faunus id with the blueprints id for all shell vertices (vertices incoming adjacent)
            final java.util.Map<Long, Object> faunusBlueprintsIdMap = new HashMap<Long, Object>();
            for (final Holder<FaunusVertex> holder : values) {
                if (holder.getTag() == 's') {
                    faunusBlueprintsIdMap.put(holder.get().getIdAsLong(), holder.get().getProperty(BLUEPRINTS_ID));
                } else {
                    faunusVertex.addAll(holder.get());
                }
            }

            Vertex blueprintsVertex = this.graph.getVertex(faunusVertex.getProperty(BLUEPRINTS_ID));
            for (final Edge faunusEdge : faunusVertex.getEdges(OUT)) {

                final Edge blueprintsEdge = this.graph.addEdge(null, blueprintsVertex, this.graph.getVertex(faunusBlueprintsIdMap.get(faunusEdge.getVertex(IN).getId())), faunusEdge.getLabel());
                context.getCounter(Counters.EDGES_WRITTEN).increment(1l);
                for (final String property : faunusEdge.getPropertyKeys()) {
                    blueprintsEdge.setProperty(property, faunusEdge.getProperty(property));
                    context.getCounter(Counters.EDGE_PROPERTIES_WRITTEN).increment(1l);
                }
                // after so many mutations, successfully commit the transaction (if graph is transactional)
                // for titan, if the transaction is committed, need to 'reget' the vertex
                if (BlueprintsGraphOutputMapReduce.commitGraph(this.graph, ++this.mutations))
                    blueprintsVertex = this.graph.getVertex(blueprintsVertex.getId());
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
