package com.thinkaurelius.faunus.mapreduce.filter;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import groovy.lang.Closure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FilterMap {

    public static final String CLASS = Tokens.makeNamespace(FilterMap.class) + ".class";
    public static final String CLOSURE = Tokens.makeNamespace(FilterMap.class) + ".closure";
    private static final ScriptEngine engine = new GremlinGroovyScriptEngine();

    public enum Counters {
        VERTICES_FILTERED,
        EDGES_FILTERED
    }

    public static Configuration createConfiguration(final Class<? extends Element> klass, final String closure) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setClass(CLASS, klass, Element.class);
        configuration.set(CLOSURE, closure);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private boolean isVertex;
        private Closure<Boolean> closure;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
            try {
                this.closure = (Closure<Boolean>) engine.eval(context.getConfiguration().get(CLOSURE));
            } catch (final ScriptException e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {

            if (this.isVertex) {
                if (value.hasPaths() && !this.closure.call(value)) {
                    value.clearPaths();
                    context.getCounter(Counters.VERTICES_FILTERED).increment(1l);
                }
            } else {
                long counter = 0;
                for (final Edge e : value.getEdges(Direction.BOTH)) {
                    final FaunusEdge edge = (FaunusEdge) e;
                    if (edge.hasPaths() && !this.closure.call(edge)) {
                        edge.clearPaths();
                        counter++;
                    }
                }
                context.getCounter(Counters.EDGES_FILTERED).increment(counter);
            }

            context.write(NullWritable.get(), value);
        }
    }
}
