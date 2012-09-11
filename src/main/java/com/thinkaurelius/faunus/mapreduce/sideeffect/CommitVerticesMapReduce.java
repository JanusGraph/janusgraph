package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CommitVerticesMapReduce {

    public static final String ACTION = Tokens.makeNamespace(CommitVerticesMapReduce.class) + ".action";

    public enum Counters {
        VERTICES_KEPT,
        VERTICES_DROPPED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private boolean drop;

        private FaunusVertex vertex;
        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.drop = Tokens.Action.valueOf(context.getConfiguration().get(ACTION)).equals(Tokens.Action.DROP);
            this.vertex = new FaunusVertex(context.getConfiguration().getBoolean(FaunusCompiler.PATH_ENABLED, false));
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            final boolean keep;
            final boolean hasPaths = value.hasPaths();

            long verticesKept = 0;
            long verticesDropped = 0;

            if (this.drop && hasPaths)
                keep = false;
            else if (!this.drop && hasPaths)
                keep = true;
            else
                keep = this.drop && !hasPaths;

            if (keep) {
                this.longWritable.set(value.getIdAsLong());
                context.write(this.longWritable, this.holder.set('v', value));
                verticesKept++;
            } else {
                final long vertexId = value.getIdAsLong();
                this.vertex.reuse(vertexId);
                this.holder.set('k', this.vertex);
                for (final Edge edge : value.getEdges(OUT)) {
                    final Long id = (Long) edge.getVertex(IN).getId();
                    if (!id.equals(vertexId)) {
                        this.longWritable.set(id);
                        context.write(this.longWritable, this.holder);
                    }
                }
                for (final Edge edge : value.getEdges(IN)) {
                    final Long id = (Long) edge.getVertex(OUT).getId();
                    if (!id.equals(vertexId)) {
                        this.longWritable.set(id);
                        context.write(this.longWritable, this.holder);
                    }
                }
                verticesDropped++;
            }

            context.getCounter(Counters.VERTICES_DROPPED).increment(verticesDropped);
            context.getCounter(Counters.VERTICES_KEPT).increment(verticesKept);
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            FaunusVertex vertex = null;
            final Set<Long> ids = new HashSet<Long>();
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'k') {
                    ids.add(holder.get().getIdAsLong());
                    // todo: once vertex is found, do individual removes to save memory
                } else {
                    vertex = (FaunusVertex) holder.get();
                }
            }
            if (null != vertex) {
                if (ids.size() > 0)
                    vertex.removeEdgesToFrom(ids);
                context.write(NullWritable.get(), vertex);
            }
        }
    }

}
