package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static com.tinkerpop.blueprints.Direction.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VerticesVerticesMapReduce {

    public static final String DIRECTION = Tokens.makeNamespace(VerticesVerticesMapReduce.class) + ".direction";
    public static final String LABELS = Tokens.makeNamespace(VerticesVerticesMapReduce.class) + ".labels";

    public enum Counters {
        EDGES_TRAVERSED
    }

    public static Configuration createConfiguration(final Direction direction, final String... labels) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.set(DIRECTION, direction.name());
        configuration.setStrings(LABELS, labels);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private Direction direction;
        private String[] labels;

        private FaunusVertex vertex;
        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();
        private final LongWritable longWritable = new LongWritable();


        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
            this.labels = context.getConfiguration().getStrings(LABELS, new String[0]);
            this.vertex = new FaunusVertex(context.getConfiguration().getBoolean(FaunusCompiler.PATH_ENABLED, false));
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {

            if (value.hasPaths()) {
                long edgesTraversed = 0l;
                if (this.direction.equals(OUT) || this.direction.equals(BOTH)) {
                    for (final Edge edge : value.getEdges(OUT, this.labels)) {
                        this.vertex.reuse(((FaunusEdge) edge).getVertexId(IN));
                        this.vertex.getPaths(value, false);
                        this.longWritable.set(this.vertex.getIdAsLong());
                        context.write(this.longWritable, this.holder.set('p', this.vertex));
                        edgesTraversed++;
                    }
                }

                if (this.direction.equals(IN) || this.direction.equals(BOTH)) {
                    for (final Edge edge : value.getEdges(IN, this.labels)) {
                        this.vertex.reuse(((FaunusEdge) edge).getVertexId(OUT));
                        this.vertex.getPaths(value, false);
                        this.longWritable.set(this.vertex.getIdAsLong());
                        context.write(this.longWritable, this.holder.set('p', this.vertex));
                        edgesTraversed++;
                    }
                }
                value.clearPaths();
                context.getCounter(Counters.EDGES_TRAVERSED).increment(edgesTraversed);
            }

            this.longWritable.set(value.getIdAsLong());
            context.write(this.longWritable, this.holder.set('v', value));
        }
    }

    public static class Combiner extends Reducer<LongWritable, Holder, LongWritable, Holder> {
        private FaunusVertex vertex;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.vertex = new FaunusVertex(context.getConfiguration().getBoolean(FaunusCompiler.PATH_ENABLED, false));
        }

        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            this.vertex.reuse(key.get());
            char outTag = 'x';
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    this.vertex.addAll((FaunusVertex) holder.get());
                    outTag = 'v';
                } else if (tag == 'p') {
                    this.vertex.getPaths(holder.get(), true);
                } else {
                    this.vertex.getPaths(holder.get(), false);
                }
            }
            context.write(key, this.holder.set(outTag, this.vertex));
        }

    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        private FaunusVertex vertex;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.vertex = new FaunusVertex(context.getConfiguration().getBoolean(FaunusCompiler.PATH_ENABLED, false));
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            this.vertex.reuse(key.get());
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    this.vertex.addAll((FaunusVertex) holder.get());
                } else if (tag == 'p') {
                    this.vertex.getPaths(holder.get(), true);
                } else {
                    this.vertex.getPaths(holder.get(), false);
                }
            }
            context.write(NullWritable.get(), this.vertex);
        }
    }
}