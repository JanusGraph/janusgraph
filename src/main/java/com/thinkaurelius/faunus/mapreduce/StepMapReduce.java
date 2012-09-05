package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import groovy.lang.Closure;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StepMapReduce {

    public static final String CLASS = Tokens.makeNamespace(StepMapReduce.class) + ".class";
    public static final String MAP_CLOSURE = Tokens.makeNamespace(StepMapReduce.class) + ".mapClosure";
    public static final String REDUCE_CLOSURE = Tokens.makeNamespace(StepMapReduce.class) + ".reduceClosure";
    private static final ScriptEngine engine = new GremlinGroovyScriptEngine();

    public enum Counters {
        VERTICES_PROCESSED,
        EDGES_PROCESSED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, WritableComparable, WritableComparable> {

        private Closure mapClosure;
        private boolean isVertex;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
            try {
                this.mapClosure = (Closure) engine.eval(context.getConfiguration().get(MAP_CLOSURE));
            } catch (final ScriptException e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, WritableComparable, WritableComparable>.Context context) throws IOException, InterruptedException {
            if (this.isVertex) {
                if (value.hasPaths()) {
                    for (int i = 0; i < value.pathCount(); i++) {
                        this.mapClosure.call(value, context);
                    }
                    context.getCounter(Counters.VERTICES_PROCESSED).increment(1l);
                }
            } else {
                long edgesProcessed = 0;
                for (final Edge e : value.getEdges(Direction.OUT)) {
                    final FaunusEdge edge = (FaunusEdge) e;
                    if (edge.hasPaths()) {
                        for (int i = 0; i < edge.pathCount(); i++) {
                            this.mapClosure.call(edge, context);
                        }
                        edgesProcessed++;
                    }
                }
                context.getCounter(Counters.EDGES_PROCESSED).increment(edgesProcessed);
            }
        }
    }

    public static class Reduce extends Reducer<WritableComparable, WritableComparable, WritableComparable, WritableComparable> {

        private Closure reduceClosure;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            try {
                this.reduceClosure = (Closure) engine.eval(context.getConfiguration().get(REDUCE_CLOSURE));
            } catch (final ScriptException e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        @Override
        public void reduce(final WritableComparable key, final Iterable<WritableComparable> values, final Reducer<WritableComparable, WritableComparable, WritableComparable, WritableComparable>.Context context) throws IOException, InterruptedException {
            this.reduceClosure.call(key, values, context);
        }

    }


}
