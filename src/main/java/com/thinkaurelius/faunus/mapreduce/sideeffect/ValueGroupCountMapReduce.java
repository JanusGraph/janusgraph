package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.util.CounterMap;
import com.thinkaurelius.faunus.mapreduce.util.ElementPicker;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.thinkaurelius.faunus.mapreduce.util.SafeMapperOutputs;
import com.thinkaurelius.faunus.mapreduce.util.SafeReducerOutputs;
import com.thinkaurelius.faunus.mapreduce.util.WritableHandler;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ValueGroupCountMapReduce {

    public static final String PROPERTY = Tokens.makeNamespace(ValueGroupCountMapReduce.class) + ".property";
    public static final String CLASS = Tokens.makeNamespace(ValueGroupCountMapReduce.class) + ".class";
    public static final String TYPE = Tokens.makeNamespace(ValueGroupCountMapReduce.class) + ".type";

    public enum Counters {
        PROPERTIES_COUNTED
    }

    public static Configuration createConfiguration(final Class<? extends Element> klass, final String key, final Class<? extends Writable> type) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setClass(CLASS, klass, Element.class);
        configuration.set(PROPERTY, key);
        configuration.setClass(TYPE, type, Writable.class);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, WritableComparable, LongWritable> {

        private String property;
        private WritableHandler handler;
        private boolean isVertex;
        // making use of in-map aggregation/combiner
        private CounterMap<Object> map;
        private int mapSpillOver;

        private SafeMapperOutputs outputs;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.map = new CounterMap<Object>();
            this.mapSpillOver = context.getConfiguration().getInt(Tokens.FAUNUS_PIPELINE_MAP_SPILL_OVER, Tokens.DEFAULT_MAP_SPILL_OVER);
            this.property = context.getConfiguration().get(PROPERTY);
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
            this.handler = new WritableHandler(context.getConfiguration().getClass(TYPE, Text.class, WritableComparable.class));
            this.outputs = new SafeMapperOutputs(context);

        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, WritableComparable, LongWritable>.Context context) throws IOException, InterruptedException {

            if (this.isVertex) {
                if (value.hasPaths()) {
                    this.map.incr(ElementPicker.getProperty(value, this.property), value.pathCount());
                    context.getCounter(Counters.PROPERTIES_COUNTED).increment(1l);
                }
            } else {
                for (final Edge e : value.getEdges(Direction.OUT)) {
                    final FaunusEdge edge = (FaunusEdge) e;
                    if (edge.hasPaths()) {
                        this.map.incr(ElementPicker.getProperty(edge, this.property), edge.pathCount());
                        context.getCounter(Counters.PROPERTIES_COUNTED).increment(1l);
                    }
                }
            }

            // protected against memory explosion
            if (this.map.size() > this.mapSpillOver) {
                this.dischargeMap(context);
            }

            this.outputs.write(Tokens.GRAPH, NullWritable.get(), value);

        }

        private final LongWritable longWritable = new LongWritable();

        public void dischargeMap(final Mapper<NullWritable, FaunusVertex, WritableComparable, LongWritable>.Context context) throws IOException, InterruptedException {
            for (final java.util.Map.Entry<Object, Long> entry : this.map.entrySet()) {
                this.longWritable.set(entry.getValue());
                context.write(this.handler.set(entry.getKey()), this.longWritable);
            }
            this.map.clear();
        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, WritableComparable, LongWritable>.Context context) throws IOException, InterruptedException {
            this.dischargeMap(context);
            this.outputs.close();
        }
    }

    public static class Combiner extends Reducer<WritableComparable, LongWritable, WritableComparable, LongWritable> {

        private final LongWritable longWritable = new LongWritable();

        @Override
        public void reduce(final WritableComparable key, final Iterable<LongWritable> values, final Reducer<WritableComparable, LongWritable, WritableComparable, LongWritable>.Context context) throws IOException, InterruptedException {
            long totalCount = 0;
            for (final LongWritable token : values) {
                totalCount = totalCount + token.get();
            }
            this.longWritable.set(totalCount);
            context.write(key, this.longWritable);
        }
    }

    public static class Reduce extends Reducer<WritableComparable, LongWritable, WritableComparable, LongWritable> {

        private SafeReducerOutputs outputs;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.outputs = new SafeReducerOutputs(context);
        }


        private final LongWritable longWritable = new LongWritable();

        @Override
        public void reduce(final WritableComparable key, final Iterable<LongWritable> values, final Reducer<WritableComparable, LongWritable, WritableComparable, LongWritable>.Context context) throws IOException, InterruptedException {
            long totalCount = 0;
            for (final LongWritable token : values) {
                totalCount = totalCount + token.get();
            }
            this.longWritable.set(totalCount);
            this.outputs.write(Tokens.SIDEEFFECT, key, this.longWritable);
        }

        @Override
        public void cleanup(final Reducer<WritableComparable, LongWritable, WritableComparable, LongWritable>.Context context) throws IOException, InterruptedException {
            this.outputs.close();
        }
    }
}
