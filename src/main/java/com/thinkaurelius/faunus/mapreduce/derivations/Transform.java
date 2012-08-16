package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import groovy.lang.Closure;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Transform {

    public static final String CLASS = Tokens.makeNamespace(Transform.class) + ".class";
    public static final String FUNCTION = Tokens.makeNamespace(Transform.class) + ".function";
    private static final ScriptEngine engine = new GremlinGroovyScriptEngine();

    public enum Counters {
        EDGES_TRANSFORMED,
        VERTICES_TRANSFORMED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private Closure closure;
        private Class<? extends Element> klass;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            try {
                this.closure = (Closure) engine.eval(context.getConfiguration().get(FUNCTION));
            } catch (final ScriptException e) {
                throw new IOException(e.getMessage(), e);
            }
            this.klass = context.getConfiguration().getClass(CLASS, Element.class, Element.class);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (this.klass.equals(Vertex.class)) {
                this.closure.call(value);
                context.getCounter(Counters.VERTICES_TRANSFORMED).increment(1l);
            } else if (this.klass.equals(Edge.class)) {
                for (final Edge edge : value.getEdges(Direction.BOTH)) {
                    this.closure.call(edge);
                    context.getCounter(Counters.EDGES_TRANSFORMED).increment(1l);
                }
            } else {
                throw new IOException("Unsupported element class: " + this.klass);
            }
            context.write(NullWritable.get(), value);
        }
    }
}
