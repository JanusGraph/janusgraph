package com.thinkaurelius.titan.hadoop.mapreduce.sideeffect;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.hadoop.*;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.mapreduce.util.CounterMap;
import com.tinkerpop.blueprints.Direction;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LinkMapReduce {

//    public static final String DIRECTION = Tokens.makeNamespace(LinkMapReduce.class) + ".direction";
//    public static final String LABEL = Tokens.makeNamespace(LinkMapReduce.class) + ".label";
//    public static final String STEP = Tokens.makeNamespace(LinkMapReduce.class) + ".step";
//    public static final String MERGE_DUPLICATES = Tokens.makeNamespace(LinkMapReduce.class) + ".mergeDuplicates";
//    public static final String MERGE_WEIGHT_KEY = Tokens.makeNamespace(LinkMapReduce.class) + ".mergeWeightKey";

    public static final String NO_WEIGHT_KEY = "_";

    public enum Counters {
        IN_EDGES_CREATED,
        OUT_EDGES_CREATED
    }

    public static org.apache.hadoop.conf.Configuration createConfiguration(final Direction direction, final String label, final int step, final String mergeWeightKey) {
        ModifiableHadoopConfiguration c = ModifiableHadoopConfiguration.withoutResources();

        c.set(LINK_STEP, step);
        c.set(LINK_DIRECTION, direction);
        c.set(LINK_LABEL, label);

        if (null == mergeWeightKey) {
            c.set(LINK_MERGE_DUPLICATES, false);
            c.set(LINK_MERGE_WEIGHT_KEY, NO_WEIGHT_KEY);
        } else {
            c.set(LINK_MERGE_DUPLICATES, true);
            c.set(LINK_MERGE_WEIGHT_KEY, mergeWeightKey);
        }

        c.set(PIPELINE_TRACK_PATHS, true);

        return c.getHadoopConfiguration();
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private Direction direction;
        private String label;
        private int step;
        private final Holder<FaunusPathElement> holder = new Holder<FaunusPathElement>();
        private final LongWritable longWritable = new LongWritable();
        private boolean mergeDuplicates;
        private String mergeWeightKey;
        private Configuration faunusConf;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));

            if (!faunusConf.get(PIPELINE_TRACK_PATHS))
                throw new IllegalStateException(LinkMapReduce.class.getSimpleName() + " requires that paths be enabled");

            step = faunusConf.get(LINK_STEP);
            direction = faunusConf.get(LINK_DIRECTION);
            label = faunusConf.get(LINK_LABEL);
            mergeDuplicates = faunusConf.get(LINK_MERGE_DUPLICATES);
            mergeWeightKey = faunusConf.get(LINK_MERGE_WEIGHT_KEY);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            final long valueId = value.getLongId();

            if (value.hasPaths()) {
                long edgesCreated = 0;
                if (mergeDuplicates) {
                    final CounterMap<Long> map = new CounterMap<Long>();
                    for (final List<FaunusPathElement.MicroElement> path : value.getPaths()) {
                        map.incr(path.get(step).getId(), 1);
                    }
                    for (java.util.Map.Entry<Long, Long> entry : map.entrySet()) {
                        final long linkElementId = entry.getKey();
                        final StandardFaunusEdge edge;
                        if (direction.equals(IN))
                            edge = new StandardFaunusEdge(faunusConf, linkElementId, valueId, label);
                        else
                            edge = new StandardFaunusEdge(faunusConf, valueId, linkElementId, label);

                        if (!mergeWeightKey.equals(NO_WEIGHT_KEY))
                            edge.setProperty(mergeWeightKey, entry.getValue());

                        value.addEdge(direction, edge);
                        edgesCreated++;
                        longWritable.set(linkElementId);
                        context.write(longWritable, holder.set('e', edge));
                    }
                } else {
                    for (final List<FaunusPathElement.MicroElement> path : value.getPaths()) {
                        final long linkElementId = path.get(step).getId();
                        final StandardFaunusEdge edge;
                        if (direction.equals(IN))
                            edge = new StandardFaunusEdge(faunusConf, linkElementId, valueId, label);
                        else
                            edge = new StandardFaunusEdge(faunusConf, valueId, linkElementId, label);

                        value.addEdge(direction, edge);
                        edgesCreated++;
                        longWritable.set(linkElementId);
                        context.write(longWritable, holder.set('e', edge));
                    }
                }
                if (direction.equals(OUT)) {
                    DEFAULT_COMPAT.incrementContextCounter(context, Counters.OUT_EDGES_CREATED, edgesCreated);
                } else {
                    DEFAULT_COMPAT.incrementContextCounter(context, Counters.IN_EDGES_CREATED, edgesCreated);
                }

            }

            longWritable.set(valueId);
            context.write(longWritable, holder.set('v', value));
        }
    }

    public static class Combiner extends Reducer<LongWritable, Holder, LongWritable, Holder> {

        private Direction direction;
        private Configuration faunusConf;

        private static final Logger log =
                LoggerFactory.getLogger(Combiner.class);

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));

            if (!faunusConf.has(LINK_DIRECTION)) {
                Iterator<Entry<String, String>> it = context.getConfiguration().iterator();
                log.error("Broken configuration missing {}", LINK_DIRECTION);
                log.error("---- Start config dump ----");
                while (it.hasNext()) {
                    Entry<String,String> ent = it.next();
                    log.error("k:{} -> v:{}", ent.getKey(), ent.getValue());
                }
                log.error("---- End config dump   ----");
                throw new NullPointerException();
            }
            direction = faunusConf.get(LINK_DIRECTION).opposite();
        }

        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            long edgesCreated = 0;
            final FaunusVertex vertex = new FaunusVertex(faunusConf, key.get());
            char outTag = 'x';
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    vertex.addAll((FaunusVertex) holder.get());
                    outTag = 'v';
                } else if (tag == 'e') {
                    vertex.addEdge(direction, (StandardFaunusEdge) holder.get());
                    edgesCreated++;
                } else {
                    vertex.addEdges(Direction.BOTH, (FaunusVertex) holder.get());
                }
            }

            context.write(key, holder.set(outTag, vertex));

            if (direction.equals(OUT)) {
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.OUT_EDGES_CREATED, edgesCreated);
            } else {
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.IN_EDGES_CREATED, edgesCreated);
            }
        }

    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        private Direction direction;
        private Configuration faunusConf;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));
            direction = faunusConf.get(LINK_DIRECTION).opposite();
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            long edgesCreated = 0;
            final FaunusVertex vertex = new FaunusVertex(faunusConf, key.get());
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    vertex.addAll((FaunusVertex) holder.get());
                } else if (tag == 'e') {
                    vertex.addEdge(direction, (StandardFaunusEdge) holder.get());
                    edgesCreated++;
                } else {
                    vertex.addEdges(Direction.BOTH, (FaunusVertex) holder.get());
                }
            }
            context.write(NullWritable.get(), vertex);

            if (direction.equals(OUT)) {
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.OUT_EDGES_CREATED, edgesCreated);
            } else {
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.IN_EDGES_CREATED, edgesCreated);
            }
        }
    }
}