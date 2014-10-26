package com.thinkaurelius.titan.hadoop.mapreduce.transform;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.Holder;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static com.tinkerpop.blueprints.Direction.*;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VerticesVerticesMapReduce {

//    public static final String DIRECTION = Tokens.makeNamespace(VerticesVerticesMapReduce.class) + ".direction";
//    public static final String LABELS = Tokens.makeNamespace(VerticesVerticesMapReduce.class) + ".labels";

    public enum Counters {
        EDGES_TRAVERSED
    }

    public static org.apache.hadoop.conf.Configuration createConfiguration(final Direction direction, final String... labels) {
        ModifiableHadoopConfiguration c = ModifiableHadoopConfiguration.withoutResources();
        c.set(VERTICES_VERTICES_DIRECTION, direction);
        c.set(VERTICES_VERTICES_LABELS, labels);
        return c.getHadoopConfiguration();
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private Direction direction;
        private String[] labels;
        private Configuration faunusConf;

        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();
        private final LongWritable longWritable = new LongWritable();


        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getJobContextConfiguration(context));
            direction = faunusConf.get(VERTICES_VERTICES_DIRECTION);
            labels = faunusConf.get(VERTICES_VERTICES_LABELS);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {

            if (value.hasPaths()) {
                long edgesTraversed = 0l;
                if (this.direction.equals(OUT) || this.direction.equals(BOTH)) {
                    for (final Edge edge : value.getEdges(OUT, this.labels)) {
                        final FaunusVertex vertex = new FaunusVertex(faunusConf, ((StandardFaunusEdge) edge).getVertexId(IN));
                        vertex.getPaths(value, false);
                        this.longWritable.set(vertex.longId());
                        context.write(this.longWritable, this.holder.set('p', vertex));
                        edgesTraversed++;
                    }
                }

                if (this.direction.equals(IN) || this.direction.equals(BOTH)) {
                    for (final Edge edge : value.getEdges(IN, this.labels)) {
                        final FaunusVertex vertex = new FaunusVertex(faunusConf, ((StandardFaunusEdge) edge).getVertexId(OUT));
                        vertex.getPaths(value, false);
                        this.longWritable.set(vertex.longId());
                        context.write(this.longWritable, this.holder.set('p', vertex));
                        edgesTraversed++;
                    }
                }
                value.clearPaths();
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.EDGES_TRAVERSED, edgesTraversed);
            }

            this.longWritable.set(value.longId());
            context.write(this.longWritable, this.holder.set('v', value));
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        private Configuration faunusConf;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getJobContextConfiguration(context));
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(faunusConf, key.get());
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    vertex.addAll((FaunusVertex) holder.get());
                } else if (tag == 'p') {
                    vertex.getPaths(holder.get(), true);
                } else {
                    vertex.getPaths(holder.get(), false);
                }
            }
            context.write(NullWritable.get(), vertex);
        }
    }
}