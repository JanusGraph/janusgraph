package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import groovy.lang.Closure;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeFilter {

    public static final String FUNCTION = Tokens.makeNamespace(EdgeFilter.class) + ".function";
    private static final ScriptEngine engine = new GremlinGroovyScriptEngine();

    public enum Counters {
        EDGES_KEPT,
        EDGES_DROPPED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private Closure<Boolean> closure;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            try {
                this.closure = (Closure<Boolean>) engine.eval(context.getConfiguration().get(FUNCTION));
            } catch (final ScriptException e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final Iterator<Edge> itty = value.getEdges(Direction.BOTH).iterator();
            while (itty.hasNext()) {
                final Edge edge = itty.next();
                if (this.closure.call(edge))
                    context.getCounter(Counters.EDGES_KEPT).increment(1l);
                else {
                    itty.remove();
                    context.getCounter(Counters.EDGES_DROPPED).increment(1l);
                }
            }
            context.write(NullWritable.get(), value);
        }
    }
}
