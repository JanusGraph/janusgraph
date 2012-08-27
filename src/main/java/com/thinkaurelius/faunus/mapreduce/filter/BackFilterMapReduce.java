package com.thinkaurelius.faunus.mapreduce.filter;

import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.FaunusRunner;
import com.thinkaurelius.faunus.util.MicroElement;
import com.tinkerpop.blueprints.Direction;
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

    public enum Counters {
        VERTICES_FILTERED,
        EDGES_FILTERED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private int step;
        private boolean isVertex;
        private final FaunusVertex vertex = new FaunusVertex();
        private final Holder<FaunusElement> holder = new Holder<FaunusElement>();
        private final LongWritable longWritable = new LongWritable();


        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.step = context.getConfiguration().getInt(context.getConfiguration().get(FaunusRunner.TAG + "." + context.getConfiguration().get(STEP)), 0);
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            if (this.isVertex) {
                if (value.hasPaths()) {
                    for (final List<MicroElement> path : value.getPaths()) {
                        final long backElementId = path.get(this.step).getId();
                        this.longWritable.set(backElementId);
                        this.vertex.reuse(backElementId);
                        this.vertex.addPath(path, false);
                        context.write(this.longWritable, this.holder.set('p', this.vertex));
                    }
                }
            } else {
                // TODO: back up edges
            }

            value.clearPaths();
            this.longWritable.set(value.getIdAsLong());
            context.write(this.longWritable, this.holder.set('v', value));
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(key.get());
            for (final Holder holder : values) {
                if (holder.getTag() == 'v') {
                    final FaunusVertex temp = (FaunusVertex) holder.get();
                    vertex.addEdges(Direction.BOTH, temp);
                    vertex.setProperties(temp.getProperties());
                } else {
                    vertex.addPaths(holder.get().getPaths(), true);
                }
            }
            context.write(NullWritable.get(), vertex);
        }
    }
}
