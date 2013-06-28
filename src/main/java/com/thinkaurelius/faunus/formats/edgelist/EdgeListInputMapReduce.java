package com.thinkaurelius.faunus.formats.edgelist;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeListInputMapReduce {

    public enum Counters {
        EDGES_PROCESSED,
        VERTICES_EMITTED,
        IN_EDGES_CREATED,
        OUT_EDGES_CREATED,
        VERTICES_CREATED,
        VERTEX_PROPERTIES_CREATED
    }

    public static Configuration createConfiguration() {
        return new EmptyConfiguration();
    }

    public static class Map extends Mapper<NullWritable, FaunusElement, LongWritable, FaunusVertex> {

        private final HashMap<Long, FaunusVertex> map = new HashMap<Long, FaunusVertex>();
        private static final int MAX_MAP_SIZE = 5000;
        private final LongWritable longWritable = new LongWritable();
        private int counter = 0;

        @Override
        public void map(final NullWritable key, final FaunusElement value, final Mapper<NullWritable, FaunusElement, LongWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (value instanceof FaunusEdge) {
                final long outId = ((FaunusEdge) value).getVertexId(OUT);
                final long inId = ((FaunusEdge) value).getVertexId(IN);
                FaunusVertex vertex = this.map.get(outId);
                if (null == vertex) {
                    vertex = new FaunusVertex(outId);
                    this.map.put(outId, vertex);
                }
                vertex.addEdge(OUT, WritableUtils.clone((FaunusEdge) value, context.getConfiguration()));
                this.counter++;

                vertex = this.map.get(inId);
                if (null == vertex) {
                    vertex = new FaunusVertex(inId);
                    this.map.put(inId, vertex);
                }
                vertex.addEdge(IN, WritableUtils.clone((FaunusEdge) value, context.getConfiguration()));
                context.getCounter(Counters.EDGES_PROCESSED).increment(1l);
                this.counter++;
            } else {
                final long id = value.getIdAsLong();
                FaunusVertex vertex = this.map.get(id);
                if (null == vertex) {
                    vertex = new FaunusVertex(id);
                    this.map.put(id, vertex);
                }
                vertex.getProperties().putAll(value.getProperties());
                vertex.addEdges(BOTH, WritableUtils.clone((FaunusVertex) value, context.getConfiguration()));
                this.counter++;
            }
            if (this.counter > MAX_MAP_SIZE)
                this.flush(context);
        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusElement, LongWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            this.flush(context);
        }

        private void flush(final Mapper<NullWritable, FaunusElement, LongWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            for (final FaunusVertex vertex : this.map.values()) {
                this.longWritable.set(vertex.getIdAsLong());
                context.write(this.longWritable, vertex);
                context.getCounter(Counters.VERTICES_EMITTED).increment(1l);
            }
            this.map.clear();
            this.counter = 0;
        }
    }

    public static class Combiner extends Reducer<LongWritable, FaunusVertex, LongWritable, FaunusVertex> {

        private final FaunusVertex vertex = new FaunusVertex();

        @Override
        public void reduce(final LongWritable key, final Iterable<FaunusVertex> values, final Reducer<LongWritable, FaunusVertex, LongWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            this.vertex.reuse(key.get());
            for (final FaunusVertex value : values) {
                this.vertex.addEdges(BOTH, value);
                this.vertex.getProperties().putAll(value.getProperties());
            }
            context.write(key, this.vertex);
        }
    }

    public static class Reduce extends Reducer<LongWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private final FaunusVertex vertex = new FaunusVertex();

        @Override
        public void reduce(final LongWritable key, final Iterable<FaunusVertex> values, final Reducer<LongWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            this.vertex.reuse(key.get());
            for (final FaunusVertex value : values) {
                this.vertex.addEdges(BOTH, value);
                this.vertex.getProperties().putAll(value.getProperties());
            }
            context.getCounter(Counters.VERTICES_CREATED).increment(1l);
            context.getCounter(Counters.VERTEX_PROPERTIES_CREATED).increment(this.vertex.getProperties().size());
            context.getCounter(Counters.OUT_EDGES_CREATED).increment(((List) this.vertex.getEdges(OUT)).size());
            context.getCounter(Counters.IN_EDGES_CREATED).increment(((List) this.vertex.getEdges(IN)).size());
            context.write(NullWritable.get(), this.vertex);
        }
    }
}
