package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import groovy.lang.Closure;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TransformMap {

    public static final String FUNCTION = Tokens.makeNamespace(TransformMap.class) + ".function";
    private static final ScriptEngine engine = new GremlinGroovyScriptEngine();

    public enum Counters {
        VERTICES_PROCESSED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, FaunusVertex, Text> {

        private Closure closure;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            try {
                this.closure = (Closure) engine.eval(context.getConfiguration().get(FUNCTION));
            } catch (final ScriptException e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        private final Text textWritable = new Text();

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, FaunusVertex, Text>.Context context) throws IOException, InterruptedException {
            final Object result = this.closure.call(value);
            this.textWritable.set(null == result ? Tokens.NULL : result.toString());
            context.write(value, this.textWritable);
            context.getCounter(Counters.VERTICES_PROCESSED).increment(1l);
        }
    }
}
