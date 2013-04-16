package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.mapreduce.util.CounterMap;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LinkMapReduce {

    public static final String DIRECTION = Tokens.makeNamespace(LinkMapReduce.class) + ".direction";
    public static final String LABEL = Tokens.makeNamespace(LinkMapReduce.class) + ".label";
    public static final String STEP = Tokens.makeNamespace(LinkMapReduce.class) + ".step";
    public static final String MERGE_DUPLICATES = Tokens.makeNamespace(LinkMapReduce.class) + ".mergeDuplicates";
    public static final String MERGE_WEIGHT_KEY = Tokens.makeNamespace(LinkMapReduce.class) + ".mergeWeightKey";

    public static final String NO_WEIGHT_KEY = "_";

    public enum Counters {
        IN_EDGES_CREATED,
        OUT_EDGES_CREATED
    }

    public static Configuration createConfiguration(final Direction direction, final String label, final int step, final String mergeWeightKey) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setInt(STEP, step);
        configuration.set(DIRECTION, direction.name());
        configuration.set(LABEL, label);
        if (null == mergeWeightKey) {
            configuration.setBoolean(MERGE_DUPLICATES, false);
            configuration.set(MERGE_WEIGHT_KEY, NO_WEIGHT_KEY);
        } else {
            configuration.setBoolean(MERGE_DUPLICATES, true);
            configuration.set(MERGE_WEIGHT_KEY, mergeWeightKey);
        }
        configuration.setBoolean(FaunusCompiler.PATH_ENABLED, true);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private Direction direction;
        private String label;
        private int step;
        private final Holder<FaunusElement> holder = new Holder<FaunusElement>();
        private final LongWritable longWritable = new LongWritable();
        private boolean mergeDuplicates;
        private String mergeWeightKey;

        private boolean pathEnabled;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.step = context.getConfiguration().getInt(STEP, -1);
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
            this.label = context.getConfiguration().get(LABEL);
            this.mergeDuplicates = context.getConfiguration().getBoolean(MERGE_DUPLICATES, false);
            this.mergeWeightKey = context.getConfiguration().get(MERGE_WEIGHT_KEY, NO_WEIGHT_KEY);
            this.pathEnabled = context.getConfiguration().getBoolean(FaunusCompiler.PATH_ENABLED, false);

            if (!this.pathEnabled)
                throw new IllegalStateException(LinkMapReduce.class.getSimpleName() + " requires that paths be enabled");
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            final long valueId = value.getIdAsLong();

            if (value.hasPaths()) {
                long edgesCreated = 0;
                if (this.mergeDuplicates) {
                    final CounterMap<Long> map = new CounterMap<Long>();
                    for (final List<FaunusElement.MicroElement> path : value.getPaths()) {
                        map.incr(path.get(this.step).getId(), 1);
                    }
                    for (java.util.Map.Entry<Long, Long> entry : map.entrySet()) {
                        final long linkElementId = entry.getKey();
                        final FaunusEdge edge;
                        if (this.direction.equals(IN))
                            edge = new FaunusEdge(linkElementId, valueId, this.label);
                        else
                            edge = new FaunusEdge(valueId, linkElementId, this.label);
                        edge.enablePath(this.pathEnabled);

                        if (!this.mergeWeightKey.equals(NO_WEIGHT_KEY))
                            edge.setProperty(this.mergeWeightKey, entry.getValue());

                        value.addEdge(this.direction, edge);
                        edgesCreated++;
                        this.longWritable.set(linkElementId);
                        context.write(this.longWritable, this.holder.set('e', edge));
                    }
                } else {
                    for (final List<FaunusElement.MicroElement> path : value.getPaths()) {
                        final long linkElementId = path.get(this.step).getId();
                        final FaunusEdge edge;
                        if (this.direction.equals(IN))
                            edge = new FaunusEdge(linkElementId, valueId, this.label);
                        else
                            edge = new FaunusEdge(valueId, linkElementId, this.label);
                        edge.enablePath(this.pathEnabled);
                        value.addEdge(this.direction, edge);
                        edgesCreated++;
                        this.longWritable.set(linkElementId);
                        context.write(this.longWritable, this.holder.set('e', edge));
                    }
                }
                if (this.direction.equals(OUT))
                    context.getCounter(Counters.OUT_EDGES_CREATED).increment(edgesCreated);
                else
                    context.getCounter(Counters.IN_EDGES_CREATED).increment(edgesCreated);

            }

            this.longWritable.set(valueId);
            context.write(this.longWritable, this.holder.set('v', value));
        }
    }

    public static class Combiner extends Reducer<LongWritable, Holder, LongWritable, Holder> {

        private Direction direction;
        private FaunusVertex vertex;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(LinkMapReduce.DIRECTION));
            this.direction = this.direction.opposite();
            this.vertex = new FaunusVertex(context.getConfiguration().getBoolean(FaunusCompiler.PATH_ENABLED, false));
        }

        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            long edgesCreated = 0;
            this.vertex.reuse(key.get());
            char outTag = 'x';
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    this.vertex.addAll((FaunusVertex) holder.get());
                    outTag = 'v';
                } else if (tag == 'e') {
                    this.vertex.addEdge(this.direction, (FaunusEdge) holder.get());
                    edgesCreated++;
                } else {
                    this.vertex.addEdges(Direction.BOTH, (FaunusVertex) holder.get());
                }
            }

            context.write(key, this.holder.set(outTag, this.vertex));

            if (this.direction.equals(OUT))
                context.getCounter(Counters.OUT_EDGES_CREATED).increment(edgesCreated);
            else
                context.getCounter(Counters.IN_EDGES_CREATED).increment(edgesCreated);
        }

    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        private Direction direction;
        private FaunusVertex vertex;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
            this.direction = this.direction.opposite();
            this.vertex = new FaunusVertex(context.getConfiguration().getBoolean(FaunusCompiler.PATH_ENABLED, false));
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            long edgesCreated = 0;
            this.vertex.reuse(key.get());
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    vertex.addAll((FaunusVertex) holder.get());
                } else if (tag == 'e') {
                    vertex.addEdge(this.direction, (FaunusEdge) holder.get());
                    edgesCreated++;
                } else {
                    vertex.addEdges(Direction.BOTH, (FaunusVertex) holder.get());
                }
            }
            context.write(NullWritable.get(), vertex);

            if (this.direction.equals(OUT))
                context.getCounter(Counters.OUT_EDGES_CREATED).increment(edgesCreated);
            else
                context.getCounter(Counters.IN_EDGES_CREATED).increment(edgesCreated);
        }
    }
}