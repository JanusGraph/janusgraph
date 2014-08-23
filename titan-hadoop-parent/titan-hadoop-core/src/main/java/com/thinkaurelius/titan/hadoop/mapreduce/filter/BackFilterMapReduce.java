package com.thinkaurelius.titan.hadoop.mapreduce.filter;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.hadoop.*;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;
import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BackFilterMapReduce {

//    public static final String CLASS = Tokens.makeNamespace(BackFilterMapReduce.class) + ".class";
//    public static final String STEP = Tokens.makeNamespace(BackFilterMapReduce.class) + ".step";

    public static org.apache.hadoop.conf.Configuration createConfiguration(final Class<? extends Element> klass, final int step) {
        ModifiableHadoopConfiguration c = ModifiableHadoopConfiguration.withoutResources();
        c.set(BACK_FILTER_STEP, step);
        c.set(BACK_FILTER_CLASS, klass.getCanonicalName());
        c.set(PIPELINE_TRACK_PATHS, true);
        return c.getHadoopConfiguration();
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private int step;
        private boolean isVertex;
        private final Holder<FaunusPathElement> holder = new Holder<FaunusPathElement>();
        private final LongWritable longWritable = new LongWritable();
        private Configuration faunusConf;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));
            step = faunusConf.get(BACK_FILTER_STEP);
            String configuredClassname = faunusConf.get(BACK_FILTER_CLASS);
            isVertex = Vertex.class.getCanonicalName().equals(configuredClassname);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            if (isVertex) {
                if (value.hasPaths()) {
                    for (final List<FaunusPathElement.MicroElement> path : value.getPaths()) {
                        if (path.get(step) instanceof StandardFaunusEdge.MicroEdge)
                            throw new IOException("Back does not support backing up to previous edges");

                        final long backElementId = path.get(step).getId();
                        longWritable.set(backElementId);
                        final FaunusVertex vertex = new FaunusVertex(faunusConf, backElementId);
                        vertex.addPath(path, false);
                        context.write(longWritable, holder.set('p', vertex));
                    }
                    value.clearPaths();
                }
            } else {
                for (final Edge e : value.getEdges(Direction.OUT)) {
                    final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                    if (edge.hasPaths()) {
                        for (final List<FaunusPathElement.MicroElement> path : edge.getPaths()) {
                            if (path.get(step) instanceof StandardFaunusEdge.MicroEdge)
                                throw new IOException("Back does not support backing up to previous edges");

                            final long backElementId = path.get(step).getId();
                            longWritable.set(backElementId);
                            final FaunusVertex vertex = new FaunusVertex(faunusConf, backElementId);
                            vertex.addPath(path, false);
                            context.write(longWritable, holder.set('p', vertex));
                        }
                        edge.clearPaths();
                    }
                }

                for (final Edge e : value.getEdges(Direction.IN)) {
                    final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                    if (edge.hasPaths())
                        edge.clearPaths();
                }
            }

            longWritable.set(value.getLongId());
            context.write(longWritable, holder.set('v', value));
        }
    }

    public static class Combiner extends Reducer<LongWritable, Holder, LongWritable, Holder> {
        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();
        private Configuration faunusConf;

        @Override
        public void setup(final Combiner.Context context) {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(faunusConf, key.get());
            char outTag = 'x';
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    vertex.addAll((FaunusVertex) holder.get());
                    outTag = 'v';
                } else if (tag == 'p') {
                    vertex.getPaths(holder.get(), true);
                } else {
                    vertex.getPaths(holder.get(), false);
                }
            }
            context.write(key, holder.set(outTag, vertex));
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        private Configuration faunusConf;

        @Override
        public void setup(final Context context) {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));
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
