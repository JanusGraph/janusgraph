package com.thinkaurelius.faunus.mapreduce.statistics;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.CounterMap;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import groovy.lang.Closure;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Distribution {

    public static final String FUNCTION = Tokens.makeNamespace(Distribution.class) + ".function";
    public static final String CLASS = Tokens.makeNamespace(Distribution.class) + ".class";
    private static final ScriptEngine engine = new GremlinGroovyScriptEngine();

    public enum Counters {
        VERTICES_PROCESSED,
        EDGES_PROCESSED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, Text, LongWritable> {

        private Closure<List> closure;
        private Class<? extends Element> klass;
        private CounterMap<Object> map;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            try {
                this.closure = (Closure<List>) engine.eval(context.getConfiguration().get(FUNCTION));
            } catch (final ScriptException e) {
                throw new IOException(e.getMessage(), e);
            }
            this.klass = context.getConfiguration().getClass(CLASS, Element.class, Element.class);
            this.map = new CounterMap<Object>();
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            if (this.klass.equals(Vertex.class)) {
                final List pair = this.closure.call(value);
                this.map.incr(pair.get(0), ((Number) pair.get(1)).longValue());
                context.getCounter(Counters.VERTICES_PROCESSED).increment(1l);
            } else if (this.klass.equals(Edge.class)) {
                for (final Edge edge : value.getEdges(Direction.OUT)) {
                    final List pair = this.closure.call(edge);
                    this.map.incr(pair.get(0), ((Number) pair.get(1)).longValue());
                    context.getCounter(Counters.EDGES_PROCESSED).increment(1l);
                }
            } else {
                throw new IOException("Unsupported element class: " + this.klass.getName());
            }

            // protected against memory explosion
            if (this.map.size() > 1000) {
                this.cleanup(context);
                this.map.clear();
            }
        }


        private final Text textWritable = new Text();
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (final java.util.Map.Entry<Object, Long> entry : this.map.entrySet()) {
                this.textWritable.set(entry.getKey().toString());
                this.longWritable.set(entry.getValue());
                context.write(this.textWritable, this.longWritable);
            }
        }

    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        private final LongWritable longWritable = new LongWritable();

        @Override
        public void reduce(final Text key, final Iterable<LongWritable> values, final Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long totalDegree = 0;
            for (final LongWritable token : values) {
                totalDegree = totalDegree + token.get();
            }
            this.longWritable.set(totalDegree);
            context.write(key, this.longWritable);
        }
    }
}
