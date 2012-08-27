package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.FaunusConfiguration;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.util.MicroElement;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.IN;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LinkMapReduce {

    public static final String DIRECTION = Tokens.makeNamespace(LinkMapReduce.class) + ".direction";
    public static final String LABEL = Tokens.makeNamespace(LinkMapReduce.class) + ".label";
    public static final String STEP = Tokens.makeNamespace(LinkMapReduce.class) + ".step";

    public enum Counters {
        EDGES_CREATED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private Direction direction;
        private String label;
        private int step;
        private final Holder<FaunusElement> holder = new Holder<FaunusElement>();
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.step = context.getConfiguration().getInt(context.getConfiguration().get(FaunusCompiler.TAG + "." + context.getConfiguration().get(STEP)), 0);
            this.direction = FaunusConfiguration.getDirection(context.getConfiguration(), DIRECTION);
            this.label = context.getConfiguration().get(LABEL);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            final long valueId = value.getIdAsLong();
            if (value.hasPaths()) {
                for (final List<MicroElement> path : value.getPaths()) {
                    final long linkElementId = path.get(this.step).getId();
                    final FaunusEdge edge;
                    if (this.direction.equals(IN))
                        edge = new FaunusEdge(linkElementId, valueId, this.label);
                    else
                        edge = new FaunusEdge(valueId, linkElementId, this.label);

                    value.addEdge(this.direction, edge);
                    this.longWritable.set(linkElementId);
                    context.write(this.longWritable, this.holder.set('e', edge));
                }
            }

            this.longWritable.set(valueId);
            context.write(this.longWritable, this.holder.set('v', value));
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        private Direction direction;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.direction = FaunusConfiguration.getDirection(context.getConfiguration(), DIRECTION);
            this.direction = this.direction.opposite();
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            long edgesCreated = 0;
            final FaunusVertex vertex = new FaunusVertex(key.get());
            for (final Holder holder : values) {
                if (holder.getTag() == 'v') {
                    vertex.addAll((FaunusVertex) holder.get());
                } else {
                    vertex.addEdge(this.direction, (FaunusEdge) holder.get());
                    edgesCreated++;
                }
            }
            context.write(NullWritable.get(), vertex);
            context.getCounter(Counters.EDGES_CREATED).increment(edgesCreated);
        }
    }
}