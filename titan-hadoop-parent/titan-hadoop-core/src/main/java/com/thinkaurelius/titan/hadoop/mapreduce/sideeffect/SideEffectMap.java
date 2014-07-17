package com.thinkaurelius.titan.hadoop.mapreduce.sideeffect;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
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
public class SideEffectMap {

    public static final String CLASS = Tokens.makeNamespace(SideEffectMap.class) + ".class";
    public static final String CLOSURE = Tokens.makeNamespace(SideEffectMap.class) + ".closure";
    private static final ScriptEngine engine = new GremlinGroovyScriptEngine();

    public enum Counters {
        VERTICES_PROCESSED,
        IN_EDGES_PROCESSED,
        OUT_EDGES_PROCESSED
    }

    public static Configuration createConfiguration(final Class<? extends Element> klass, final String closure) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setClass(CLASS, klass, Element.class);
        configuration.set(CLOSURE, closure);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private Closure closure;
        private boolean isVertex;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
            try {
                this.closure = (Closure) engine.eval(context.getConfiguration().get(CLOSURE));
            } catch (final ScriptException e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (this.isVertex) {
                if (value.hasPaths()) {
                    //for (int i = 0; i < value.pathCount(); i++) {
                    this.closure.call(value);
                    //}
                    DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTICES_PROCESSED, 1L);
                }
            } else {
                long edgesProcessed = 0;
                for (final Edge e : value.getEdges(Direction.IN)) {
                    final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                    if (edge.hasPaths()) {
                        edgesProcessed++;
                        //for (int i = 0; i < edge.pathCount(); i++) {
                        this.closure.call(edge);
                        //}
                    }
                }
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.IN_EDGES_PROCESSED, edgesProcessed);

                edgesProcessed = 0;
                for (final Edge e : value.getEdges(Direction.OUT)) {
                    final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                    if (edge.hasPaths()) {
                        edgesProcessed++;
                        //for (int i = 0; i < edge.pathCount(); i++) {
                        this.closure.call(edge);
                        //}
                    }
                }
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.OUT_EDGES_PROCESSED, edgesProcessed);
            }

            context.write(NullWritable.get(), value);
        }
    }
}
