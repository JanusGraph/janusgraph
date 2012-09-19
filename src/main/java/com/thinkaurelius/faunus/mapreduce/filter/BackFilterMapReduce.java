package com.thinkaurelius.faunus.mapreduce.filter;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.util.MicroEdge;
import com.thinkaurelius.faunus.util.MicroElement;
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

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BackFilterMapReduce {

    public static final String CLASS = Tokens.makeNamespace(BackFilterMapReduce.class) + ".class";
    public static final String STEP = Tokens.makeNamespace(BackFilterMapReduce.class) + ".step";

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private int step;
        private boolean isVertex;
        private FaunusVertex vertex;
        private final Holder<FaunusElement> holder = new Holder<FaunusElement>();
        private final LongWritable longWritable = new LongWritable();


        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.step = context.getConfiguration().getInt(STEP, -1);
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
            this.vertex = new FaunusVertex(context.getConfiguration().getBoolean(FaunusCompiler.PATH_ENABLED, false));
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            if (this.isVertex) {
                if (value.hasPaths()) {
                    for (final List<MicroElement> path : value.getPaths()) {
                        if (path.get(this.step) instanceof MicroEdge)
                            throw new IOException("Back does not support backing up to previous edges");

                        final long backElementId = path.get(this.step).getId();
                        this.longWritable.set(backElementId);
                        this.vertex.reuse(backElementId);
                        this.vertex.addPath(path, false);
                        context.write(this.longWritable, this.holder.set('p', this.vertex));
                    }
                    value.clearPaths();
                }
            } else {
                for (final Edge e : value.getEdges(Direction.OUT)) {
                    final FaunusEdge edge = (FaunusEdge) e;
                    if (edge.hasPaths()) {
                        for (final List<MicroElement> path : edge.getPaths()) {
                            if (path.get(this.step) instanceof MicroEdge)
                                throw new IOException("Back does not support backing up to previous edges");

                            final long backElementId = path.get(this.step).getId();
                            this.longWritable.set(backElementId);
                            this.vertex.reuse(backElementId);
                            this.vertex.addPath(path, false);
                            context.write(this.longWritable, this.holder.set('p', this.vertex));
                        }
                        edge.clearPaths();
                    }
                }
            }

            this.longWritable.set(value.getIdAsLong());
            context.write(this.longWritable, this.holder.set('v', value));
        }
    }

    public static class Combiner extends Reducer<LongWritable, Holder, LongWritable, Holder> {
        private FaunusVertex vertex;
        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.vertex = new FaunusVertex(context.getConfiguration().getBoolean(FaunusCompiler.PATH_ENABLED, false));
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            this.vertex.reuse(key.get());
            char tag = 'p';
            for (final Holder holder : values) {
                if (holder.getTag() == 'v') {
                    this.vertex.addAll((FaunusVertex) holder.get());
                    tag = 'v';
                } else {
                    this.vertex.getPaths(holder.get(), true);
                }
            }
            context.write(key, this.holder.set(tag, this.vertex));
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
                if (holder.getTag() == 'v') {
                    this.vertex.addAll((FaunusVertex) holder.get());
                } else {
                    this.vertex.getPaths(holder.get(), true);
                }
            }
            context.write(NullWritable.get(), vertex);
        }
    }
}
