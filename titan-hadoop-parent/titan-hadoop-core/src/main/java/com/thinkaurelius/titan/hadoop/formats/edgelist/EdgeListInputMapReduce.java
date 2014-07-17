package com.thinkaurelius.titan.hadoop.formats.edgelist;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.FaunusElement;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

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
            if (value instanceof StandardFaunusEdge) {
                final long outId = ((StandardFaunusEdge) value).getVertexId(OUT);
                final long inId = ((StandardFaunusEdge) value).getVertexId(IN);
                FaunusVertex vertex = this.map.get(outId);
                if (null == vertex) {
                    vertex = new FaunusVertex(context.getConfiguration(), outId);
                    this.map.put(outId, vertex);
                }
                vertex.addEdge(OUT, WritableUtils.clone((StandardFaunusEdge) value, context.getConfiguration()));
                this.counter++;

                vertex = this.map.get(inId);
                if (null == vertex) {
                    vertex = new FaunusVertex(context.getConfiguration(), inId);
                    this.map.put(inId, vertex);
                }
                vertex.addEdge(IN, WritableUtils.clone((StandardFaunusEdge) value, context.getConfiguration()));
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.EDGES_PROCESSED, 1L);
                this.counter++;
            } else {
                final long id = value.getLongId();
                FaunusVertex vertex = this.map.get(id);
                if (null == vertex) {
                    vertex = new FaunusVertex(context.getConfiguration(), id);
                    this.map.put(id, vertex);
                }
                vertex.addAllProperties(value.getPropertyCollection());
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
                this.longWritable.set(vertex.getLongId());
                context.write(this.longWritable, vertex);
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTICES_EMITTED, 1L);
            }
            this.map.clear();
            this.counter = 0;
        }
    }

    public static class Combiner extends Reducer<LongWritable, FaunusVertex, LongWritable, FaunusVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<FaunusVertex> values, final Reducer<LongWritable, FaunusVertex, LongWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(context.getConfiguration(), key.get());
            for (final FaunusVertex value : values) {
                vertex.addEdges(BOTH, value);
                vertex.addAllProperties(value.getPropertyCollection());
            }
            context.write(key, vertex);
        }
    }

    public static class Reduce extends Reducer<LongWritable, FaunusVertex, NullWritable, FaunusVertex> {


        @Override
        public void reduce(final LongWritable key, final Iterable<FaunusVertex> values, final Reducer<LongWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(context.getConfiguration(), key.get());
            for (final FaunusVertex value : values) {
                vertex.addEdges(BOTH, value);
                vertex.addAllProperties(value.getPropertyCollection());
            }
            DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTICES_CREATED, 1L);
            DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTEX_PROPERTIES_CREATED, vertex.getPropertyCollection().size());
            DEFAULT_COMPAT.incrementContextCounter(context, Counters.OUT_EDGES_CREATED, Iterables.size(vertex.getEdges(OUT)));
            DEFAULT_COMPAT.incrementContextCounter(context, Counters.IN_EDGES_CREATED, Iterables.size(vertex.getEdges(IN)));
            context.write(NullWritable.get(), vertex);
        }
    }
}
