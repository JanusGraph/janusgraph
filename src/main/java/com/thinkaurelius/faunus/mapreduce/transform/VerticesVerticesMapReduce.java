package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusConfiguration;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VerticesVerticesMapReduce {

    public static final String DIRECTION = Tokens.makeNamespace(VerticesVerticesMapReduce.class) + ".direction";
    public static final String LABELS = Tokens.makeNamespace(VerticesVerticesMapReduce.class) + ".labels";

    public enum Counters {
        VERTICES_PROCESSED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private Direction direction;
        private String[] labels;

        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();
        private final LongWritable longWritable = new LongWritable();
        private final FaunusVertex vertex = new FaunusVertex();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = FaunusConfiguration.getDirection(context.getConfiguration(), DIRECTION);
            this.labels = context.getConfiguration().getStrings(LABELS);

        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            if (value.hasPaths()) {
                for (final Edge edge : value.getEdges(this.direction, this.labels)) {
                    this.vertex.reuse(((FaunusEdge) edge).getVertexId(this.direction.opposite()));
                    this.vertex.addPaths(value.getPaths(), false);
                    this.longWritable.set(vertex.getIdAsLong());
                    context.write(this.longWritable, this.holder.set('p', this.vertex));
                }
            }
            value.clearPaths();
            this.longWritable.set(value.getIdAsLong());
            context.write(this.longWritable, this.holder.set('v', value));
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(key.get());  // TODO: make a FaunusVertex.clear() for object reuse
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                final FaunusVertex temp = (FaunusVertex) holder.get();
                if (tag == 'v') {
                    vertex.setProperties(temp.getProperties());
                    vertex.addEdges(Direction.BOTH, temp);
                } else {
                    vertex.addPaths(temp.getPaths(), true);
                }
            }
            context.write(NullWritable.get(), vertex);
        }
    }
}