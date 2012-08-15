package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import groovy.lang.Closure;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexFilter {

    public static final String FUNCTION = Tokens.makeNamespace(VertexFilter.class) + ".function";
    private static final ScriptEngine engine = new GremlinGroovyScriptEngine();

    public enum Counters {
        VERTICES_KEPT,
        VERTICES_DROPPED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>> {

        private Closure closure;
        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            try {
                this.closure = (Closure) engine.eval(context.getConfiguration().get(FUNCTION));
            } catch (ScriptException e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) throws IOException, InterruptedException {
            if ((Boolean) this.closure.call(value)) {
                this.longWritable.set(value.getIdAsLong());
                context.write(this.longWritable, this.holder.set('v', value));
                context.getCounter(Counters.VERTICES_KEPT).increment(1l);
            } else {
                context.getCounter(Counters.VERTICES_DROPPED).increment(1l);
                this.holder.set('k', value.cloneId());
                for (final Edge edge : value.getEdges(OUT)) {
                    final Long id = (Long) edge.getVertex(IN).getId();
                    if (!id.equals(value.getId())) {
                        this.longWritable.set(id);
                        context.write(this.longWritable, this.holder);
                    }
                }
                for (final Edge edge : value.getEdges(IN)) {
                    final Long id = (Long) edge.getVertex(OUT).getId();
                    if (!id.equals(value.getId())) {
                        this.longWritable.set(id);
                        context.write(this.longWritable, this.holder);
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex> {
        @Override
        public void reduce(final LongWritable key, final Iterable<Holder<FaunusVertex>> values, final Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            FaunusVertex vertex = null;
            final Set<Long> ids = new HashSet<Long>();
            for (final Holder<FaunusVertex> holder : values) {
                final char tag = holder.getTag();
                if (tag == 'k') {
                    ids.add(holder.get().getIdAsLong());
                    // todo: once vertex is found, do individual removes to save memory
                } else if (tag == 'v') {
                    vertex = holder.get();
                } else {
                    throw new IOException("A tag of " + tag + " is not a legal tag for this operation");
                }
            }
            if (null != vertex) {
                if (ids.size() > 0)
                    vertex.removeEdgesToFrom(ids);
                context.write(NullWritable.get(), vertex);
            }
        }
    }
}
