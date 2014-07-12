package com.thinkaurelius.titan.hadoop.mapreduce.filter;

import com.thinkaurelius.titan.hadoop.*;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BackFilterMapReduce {

    public static final String CLASS = Tokens.makeNamespace(BackFilterMapReduce.class) + ".class";
    public static final String STEP = Tokens.makeNamespace(BackFilterMapReduce.class) + ".step";

    public static Configuration createConfiguration(final Class<? extends Element> klass, final int step) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setInt(STEP, step);
        configuration.setClass(CLASS, klass, Element.class);
        configuration.setBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, true);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private int step;
        private boolean isVertex;
        private final Holder<FaunusPathElement> holder = new Holder<FaunusPathElement>();
        private final LongWritable longWritable = new LongWritable();


        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.step = context.getConfiguration().getInt(STEP, -1);
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            if (this.isVertex) {
                if (value.hasPaths()) {
                    for (final List<FaunusPathElement.MicroElement> path : value.getPaths()) {
                        if (path.get(this.step) instanceof StandardFaunusEdge.MicroEdge)
                            throw new IOException("Back does not support backing up to previous edges");

                        final long backElementId = path.get(this.step).getId();
                        this.longWritable.set(backElementId);
                        final FaunusVertex vertex = new FaunusVertex(context.getConfiguration(), backElementId);
                        vertex.addPath(path, false);
                        context.write(this.longWritable, this.holder.set('p', vertex));
                    }
                    value.clearPaths();
                }
            } else {
                for (final Edge e : value.getEdges(Direction.OUT)) {
                    final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                    if (edge.hasPaths()) {
                        for (final List<FaunusPathElement.MicroElement> path : edge.getPaths()) {
                            if (path.get(this.step) instanceof StandardFaunusEdge.MicroEdge)
                                throw new IOException("Back does not support backing up to previous edges");

                            final long backElementId = path.get(this.step).getId();
                            this.longWritable.set(backElementId);
                            final FaunusVertex vertex = new FaunusVertex(context.getConfiguration(), backElementId);
                            vertex.addPath(path, false);
                            context.write(this.longWritable, this.holder.set('p', vertex));
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

            this.longWritable.set(value.getLongId());
            context.write(this.longWritable, this.holder.set('v', value));
        }
    }

    public static class Combiner extends Reducer<LongWritable, Holder, LongWritable, Holder> {
        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(context.getConfiguration(), key.get());
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
            context.write(key, this.holder.set(outTag, vertex));
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(context.getConfiguration(), key.get());
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
