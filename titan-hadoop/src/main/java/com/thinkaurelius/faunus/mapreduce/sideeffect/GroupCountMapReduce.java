package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.util.CounterMap;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.thinkaurelius.faunus.mapreduce.util.SafeMapperOutputs;
import com.thinkaurelius.faunus.mapreduce.util.SafeReducerOutputs;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import groovy.lang.Closure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountMapReduce {

    public static final String KEY_CLOSURE = Tokens.makeNamespace(GroupCountMapReduce.class) + ".keyClosure";
    public static final String VALUE_CLOSURE = Tokens.makeNamespace(GroupCountMapReduce.class) + ".valueClosure";
    public static final String CLASS = Tokens.makeNamespace(GroupCountMapReduce.class) + ".class";
    private static final ScriptEngine engine = new GremlinGroovyScriptEngine();

    public enum Counters {
        VERTICES_PROCESSED,
        OUT_EDGES_PROCESSED
    }

    public static Configuration createConfiguration(final Class<? extends Element> klass, final String keyClosure, final String valueClosure) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setClass(CLASS, klass, Element.class);
        if (null != keyClosure)
            configuration.set(KEY_CLOSURE, keyClosure);
        if (null != valueClosure)
            configuration.set(VALUE_CLOSURE, valueClosure);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, Text, LongWritable> {

        private Closure keyClosure;
        private Closure valueClosure;
        private boolean isVertex;
        private CounterMap<Object> map;

        private int mapSpillOver;

        private SafeMapperOutputs outputs;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            try {
                this.mapSpillOver = context.getConfiguration().getInt(Tokens.FAUNUS_PIPELINE_MAP_SPILL_OVER, Tokens.DEFAULT_MAP_SPILL_OVER);
                final String keyClosureString = context.getConfiguration().get(KEY_CLOSURE, null);
                if (null == keyClosureString)
                    this.keyClosure = null;
                else
                    this.keyClosure = (Closure) engine.eval(keyClosureString);

                final String valueClosureString = context.getConfiguration().get(VALUE_CLOSURE, null);
                if (null == valueClosureString)
                    this.valueClosure = null;
                else
                    this.valueClosure = (Closure) engine.eval(valueClosureString);

            } catch (final ScriptException e) {
                throw new IOException(e.getMessage(), e);
            }
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
            this.map = new CounterMap<Object>();
            this.outputs = new SafeMapperOutputs(context);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            if (this.isVertex) {
                if (value.hasPaths()) {
                    final Object object = (null == this.keyClosure) ? new FaunusVertex.MicroVertex(value.getIdAsLong()) : this.keyClosure.call(value);
                    final Number number = (null == this.valueClosure) ? 1 : (Number) this.valueClosure.call(value);
                    this.map.incr(object, number.longValue() * value.pathCount());
                    context.getCounter(Counters.VERTICES_PROCESSED).increment(1l);
                }
            } else {
                long edgesProcessed = 0;
                for (final Edge e : value.getEdges(Direction.OUT)) {
                    final FaunusEdge edge = (FaunusEdge) e;
                    if (edge.hasPaths()) {
                        final Object object = (null == this.keyClosure) ? new FaunusEdge.MicroEdge(edge.getIdAsLong()) : this.keyClosure.call(edge);
                        final Number number = (null == this.valueClosure) ? 1 : (Number) this.valueClosure.call(edge);
                        this.map.incr(object, number.longValue() * edge.pathCount());
                        edgesProcessed++;
                    }
                }
                context.getCounter(Counters.OUT_EDGES_PROCESSED).increment(edgesProcessed);
            }

            // protected against memory explosion
            if (this.map.size() > this.mapSpillOver) {
                this.dischargeMap(context);
            }

            this.outputs.write(Tokens.GRAPH, NullWritable.get(), value);
        }


        private final Text textWritable = new Text();
        private final LongWritable longWritable = new LongWritable();

        public void dischargeMap(final Mapper<NullWritable, FaunusVertex, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            for (final java.util.Map.Entry<Object, Long> entry : this.map.entrySet()) {
                this.textWritable.set(null == entry.getKey() ? Tokens.NULL : entry.getKey().toString());
                this.longWritable.set(entry.getValue());
                context.write(this.textWritable, this.longWritable);
            }
            this.map.clear();
        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            this.dischargeMap(context);
            this.outputs.close();
        }

    }


    public static class Combiner extends Reducer<Text, LongWritable, Text, LongWritable> {

        private final LongWritable longWritable = new LongWritable();

        @Override
        public void reduce(final Text key, final Iterable<LongWritable> values, final Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long totalCount = 0;
            for (final LongWritable token : values) {
                totalCount = totalCount + token.get();
            }
            this.longWritable.set(totalCount);
            context.write(key, this.longWritable);
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        private SafeReducerOutputs outputs;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.outputs = new SafeReducerOutputs(context);
        }

        private final LongWritable longWritable = new LongWritable();

        @Override
        public void reduce(final Text key, final Iterable<LongWritable> values, final Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long totalCount = 0;
            for (final LongWritable token : values) {
                totalCount = totalCount + token.get();
            }
            this.longWritable.set(totalCount);
            this.outputs.write(Tokens.SIDEEFFECT, key, this.longWritable);
        }

        @Override
        public void cleanup(final Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            this.outputs.close();
        }
    }
}
