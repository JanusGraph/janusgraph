package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.FaunusConfiguration;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
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
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LinkMapReduce {

    public static final String DIRECTION = Tokens.makeNamespace(LinkMapReduce.class) + ".direction";
    public static final String LABEL = Tokens.makeNamespace(LinkMapReduce.class) + ".label";
    public static final String STEP = Tokens.makeNamespace(LinkMapReduce.class) + ".step";

    public enum Counters {
        VERTICES_PROCESSED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private Direction direction;
        private String label;
        private int step;
        private final Holder<FaunusElement> holder = new Holder<FaunusElement>();
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.step = context.getConfiguration().getInt(STEP, 0);
            this.direction = FaunusConfiguration.getDirection(context.getConfiguration(), DIRECTION);
            this.label = context.getConfiguration().get(LABEL);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            if (value.hasPaths()) {
                for (List<Long> path : value.getPaths()) {
                    long element = path.get(this.step);
                    value.addEdge(Direction.IN, new FaunusEdge(element, value.getIdAsLong(), this.label));
                    this.longWritable.set(element);
                    context.write(this.longWritable, this.holder.set('e', new FaunusEdge(element, value.getIdAsLong(), this.label)));

                }
            }

            this.longWritable.set(value.getIdAsLong());
            context.write(this.longWritable, this.holder.set('v', value));
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {


        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(key.get());
            for (Holder holder : values) {
                if (holder.getTag() == 'v') {
                    FaunusVertex temp = (FaunusVertex) holder.get();
                    vertex.setProperties(temp.getProperties());
                    for (List<Long> path : temp.getPaths()) {
                        vertex.addPath(path);
                    }
                    for (Edge edge : temp.getEdges(Direction.OUT)) {
                        vertex.addEdge(Direction.OUT, (FaunusEdge) edge);
                    }
                    for (Edge edge : temp.getEdges(Direction.IN)) {
                        vertex.addEdge(Direction.IN, (FaunusEdge) edge);
                    }
                } else {
                    FaunusEdge edge = (FaunusEdge) holder.get();
                    vertex.addEdge(Direction.OUT, edge);
                }
            }
            context.write(NullWritable.get(), vertex);
        }
    }
}