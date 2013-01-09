package com.thinkaurelius.faunus.formats.edgelist;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static com.tinkerpop.blueprints.Direction.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeListInputMapReduce {

    public enum Counters {
        EDGES_EMITTED,
        VERTICES_EMITTED,
        IN_EDGES_CREATED,
        OUT_EDGES_CREATED,
        VERTICES_CREATED
    }

    public static class Map extends Mapper<NullWritable, FaunusElement, LongWritable, Holder> {

        private final LongWritable longWritable = new LongWritable();
        private final Holder holder = new Holder();

        @Override
        public void map(final NullWritable key, final FaunusElement value, final Mapper<NullWritable, FaunusElement, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            if (value instanceof FaunusEdge) {
                this.longWritable.set(((FaunusEdge) value).getVertexId(OUT));
                context.write(this.longWritable, this.holder.set('o', value));
                this.longWritable.set(((FaunusEdge) value).getVertexId(IN));
                context.write(this.longWritable, this.holder.set('i', value));
                context.getCounter(Counters.EDGES_EMITTED).increment(1l);
            } else {
                this.longWritable.set(value.getIdAsLong());
                context.write(this.longWritable, this.holder.set('v', value));
                context.getCounter(Counters.VERTICES_EMITTED).increment(1l);
            }
        }
    }

    public static class Combiner extends Reducer<LongWritable, Holder, LongWritable, Holder> {

        private final Holder holder = new Holder();

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(key.get());
            for (final Holder holder : values) {
                if (holder.getTag() == 'o') {
                    vertex.addEdge(OUT, (FaunusEdge) holder.get());
                    context.getCounter(Counters.OUT_EDGES_CREATED).increment(1l);
                } else if (holder.getTag() == 'i') {
                    vertex.addEdge(IN, (FaunusEdge) holder.get());
                    context.getCounter(Counters.IN_EDGES_CREATED).increment(1l);
                } else {
                    final FaunusVertex temp = (FaunusVertex) holder.get();
                    for (final String property : temp.getPropertyKeys()) {
                        vertex.setProperty(property, temp.getProperty(property));
                    }
                    vertex.addEdges(BOTH, temp);
                }
            }
            context.write(key, holder.set('v', vertex));
            context.getCounter(Counters.VERTICES_CREATED).increment(1l);
        }


    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(key.get());
            for (final Holder holder : values) {
                if (holder.getTag() == 'o') {
                    vertex.addEdge(OUT, (FaunusEdge) holder.get());
                    context.getCounter(Counters.OUT_EDGES_CREATED).increment(1l);
                } else if (holder.getTag() == 'i') {
                    vertex.addEdge(IN, (FaunusEdge) holder.get());
                    context.getCounter(Counters.IN_EDGES_CREATED).increment(1l);
                } else {
                    final FaunusVertex temp = (FaunusVertex) holder.get();
                    for (final String property : temp.getPropertyKeys()) {
                        vertex.setProperty(property, temp.getProperty(property));
                    }
                    vertex.addEdges(BOTH, temp);
                }
            }
            context.write(NullWritable.get(), vertex);
            context.getCounter(Counters.VERTICES_CREATED).increment(1l);
        }
    }
}
