package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.FaunusConfiguration;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LinkMapReduce {

    public static final String DIRECTION = Tokens.makeNamespace(LinkMapReduce.class) + ".direction";
    public static final String LABEL = Tokens.makeNamespace(LinkMapReduce.class) + ".label";
    public static final String TAG = Tokens.makeNamespace(LinkMapReduce.class) + ".tag";

    public enum Counters {
        VERTICES_PROCESSED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private char tag;
        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.tag = context.getConfiguration().get(TAG).charAt(0);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            if (value.hasEnergy() || value.getTag() == this.tag) {
                this.longWritable.set(-10l);
                if (value.getTag() == this.tag)
                    context.write(this.longWritable, holder.set('a', value));
                if (value.hasEnergy())
                    context.write(this.longWritable, holder.set('b', value));
            } else {
                this.longWritable.set(value.getIdAsLong());
                context.write(this.longWritable, holder.set('o', value));
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        private Direction direction;
        private String label;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.direction = FaunusConfiguration.getDirection(context.getConfiguration(), DIRECTION);
            this.label = context.getConfiguration().get(LABEL);
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (key.get() == -10l) {
                List<FaunusVertex> aVertices = new ArrayList<FaunusVertex>();
                List<FaunusVertex> bVertices = new ArrayList<FaunusVertex>();

                for (final Holder holder : values) {
                    if (holder.getTag() == 'a')
                        aVertices.add((FaunusVertex) holder.get());
                    else
                        bVertices.add((FaunusVertex) holder.get());
                }
                for (FaunusVertex a : aVertices) {
                    for (FaunusVertex b : bVertices) {
                        if (this.direction.equals(Direction.OUT)) {
                            final FaunusEdge edge = new FaunusEdge(a.getIdAsLong(), b.getIdAsLong(), this.label);
                            a.addEdge(Direction.OUT, edge);
                            b.addEdge(Direction.IN, edge);
                        } else {
                            final FaunusEdge edge = new FaunusEdge(b.getIdAsLong(), a.getIdAsLong(), this.label);
                            a.addEdge(Direction.IN, edge);
                            b.addEdge(Direction.OUT, edge);
                        }
                    }
                }

                for (FaunusVertex a : aVertices) {
                    context.write(NullWritable.get(), a);
                }

                for (FaunusVertex b : bVertices) {
                    context.write(NullWritable.get(), b);
                }
            } else {
                context.write(NullWritable.get(), (FaunusVertex) values.iterator().next().get());
            }
        }
    }
}
