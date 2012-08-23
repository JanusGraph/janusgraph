package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.mapreduce.FaunusRunner;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusPipeline {

    public static final String VERTEX_STATE_ERROR = "The compiler is currently in vertex state";
    public static final String EDGE_STATE_ERROR = "The compiler is currently in edge state";
    public static final String TEMP_LABEL = "_%temp%_";

    private final JobState state = new JobState();
    private final FaunusRunner compiler;

    private class JobState {
        private Class<? extends Element> elementType;
        private Queue<Direction> directions = new LinkedList<Direction>();
        private Queue<List<String>> labels = new LinkedList<List<String>>();

        public JobState set(Class<? extends Element> elementType) {
            this.elementType = elementType;
            return this;
        }

        public JobState add(final Direction direction) {
            this.directions.add(direction);
            return this;
        }

        public JobState add(final String... labels) {
            this.labels.add(Arrays.asList(labels));
            return this;
        }

        public Class<? extends Element> getElementType() {
            return this.elementType;
        }

        public boolean atVertex() {
            return this.elementType.equals(Vertex.class);
        }

        public int stackSize() {
            return this.directions.size();
        }

        public void clear() {
            this.directions.clear();
            this.labels.clear();
        }
    }

    private Query.Compare opposite(final Query.Compare compare) {
        if (compare.equals(Query.Compare.EQUAL))
            return Query.Compare.NOT_EQUAL;
        else if (compare.equals(Query.Compare.NOT_EQUAL))
            return Query.Compare.EQUAL;
        else if (compare.equals(Query.Compare.GREATER_THAN))
            return Query.Compare.LESS_THAN_EQUAL;
        else if (compare.equals(Query.Compare.GREATER_THAN_EQUAL))
            return Query.Compare.LESS_THAN;
        else if (compare.equals(Query.Compare.LESS_THAN))
            return Query.Compare.GREATER_THAN_EQUAL;
        else
            return Query.Compare.GREATER_THAN;
    }

    public FaunusPipeline(final String jobScript, final Configuration conf) {
        this.compiler = new FaunusRunner(jobScript, conf);
    }

    public FaunusPipeline _() throws IOException {
        this.compiler._();
        return this;
    }


    public FaunusPipeline V() {
        this.state.set(Vertex.class);
        return this;
    }

    public FaunusPipeline E() {
        this.state.set(Edge.class);
        return this;
    }

    public FaunusPipeline filter(final String closure) throws IOException {
        this.compiler.filter(this.state.getElementType(), closure);
        return this;
    }

    public FaunusPipeline has(final String key, final Query.Compare compare, final Object... values) throws IOException {
        this.compiler.propertyFilter(this.state.getElementType(), key, compare, values);
        return this;
    }

    public FaunusPipeline has(final String key, final Object... values) throws IOException {
        return this.has(key, Query.Compare.EQUAL, values);
    }

    public FaunusPipeline hasNot(final String key, final Object... values) throws IOException {
        return this.has(key, Query.Compare.NOT_EQUAL, values);
    }

    public FaunusPipeline hasNot(final String key, final Query.Compare compare, final Object... values) throws IOException {
        return this.has(key, this.opposite(compare), values);
    }

    public FaunusPipeline outE(final String... labels) {
        state.set(Edge.class).add(OUT).add(labels);
        return this;
    }

    public FaunusPipeline inE(final String... labels) {
        state.set(Edge.class).add(IN).add(labels);
        return this;
    }

    public FaunusPipeline inV() {
        state.set(Vertex.class);
        return this;
    }

    public FaunusPipeline outV() {
        state.set(Vertex.class);
        return this;
    }

    public FaunusPipeline out(final String... labels) {
        if (state.atVertex()) {
            state.add(OUT).add(labels);
        } else {
            throw new RuntimeException(EDGE_STATE_ERROR);
        }
        return this;
    }

    public FaunusPipeline in(final String... labels) {
        if (state.atVertex()) {
            state.add(IN).add(labels);
        } else {
            throw new RuntimeException(EDGE_STATE_ERROR);
        }
        return this;
    }

    public FaunusPipeline linkTo(final String label) throws IOException {
        if (state.atVertex()) {
            if (state.stackSize() == 1) {
                compiler.closeLine(label, Tokens.Action.KEEP, false, state.labels.remove().get(0));
            } else if (state.stackSize() > 1) {
                int maxSize = state.stackSize() - 1;
                for (int i = 0; i < maxSize; i++) {
                    final String startLabel = (i == 0) ? state.labels.remove().get(0) : TEMP_LABEL + i;
                    final Direction startDirection = (i == 0) ? state.directions.remove() : OUT;
                    final String endLabel = (i == maxSize - 1) ? label : TEMP_LABEL + (i + 1);
                    compiler.closeTriangle(startDirection, startLabel, state.directions.remove(), state.labels.remove().get(0), endLabel, Tokens.Action.KEEP);
                    // TODO: make this stage part of closeTriangle
                    if (startLabel.equals(TEMP_LABEL + i))
                        compiler.labelFilter(Tokens.Action.DROP, TEMP_LABEL + i);
                }
            } else {
                throw new RuntimeException("There are no steps to link to: " + this.state.stackSize());
            }
        } else {
            throw new RuntimeException("Edges can not be relinked");
        }
        this.state.clear();
        return this;
    }

    /*public FaunusPipeline linkFrom(final String label) throws IOException {
        if (compiler.getJobState().elementType.equals(Vertex.class)) {
            if (compiler.getJobState().directions.size() == 1 && compiler.getJobState().labels.size() == 1) {
                compiler.closeLine(compiler.getJobState().labels.get(0).get(0), label, Tokens.Action.KEEP, true);
            } else if (compiler.getJobState().directions.size() == 2 && compiler.getJobState().labels.size() == 2) {
                compiler.closeTriangle(compiler.getJobState().directions.get(0), compiler.getJobState().labels.get(0).get(0), compiler.getJobState().directions.get(1), compiler.getJobState().labels.get(1).get(0), label, Tokens.Action.KEEP);
            } else {
                throw new RuntimeException("an exception");
            }
        } else {
            throw new RuntimeException("Edges can not be relinked");
        }
        this.compiler.getJobState().directions.clear();
        this.compiler.getJobState().labels.clear();
        return this;
    }*/

    public FaunusPipeline sideEffect(final String function) throws IOException {
        if (state.getElementType().equals(Vertex.class)) {
            compiler.sideEffect(Vertex.class, function);
        } else {
            compiler.sideEffect(Edge.class, function);
        }
        return this;
    }

    public FaunusPipeline groupCount(final String keyFunction) throws IOException {
        return this.groupCount(keyFunction, "{ it -> 1l}");
    }

    public FaunusPipeline groupCount(final String keyFunction, final String valueFunction) throws IOException {
        if (state.getElementType().equals(Vertex.class)) {
            compiler.distribution(Vertex.class, keyFunction, valueFunction);
        } else {
            compiler.distribution(Edge.class, keyFunction, valueFunction);
        }
        return this;
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 1 && args.length != 2) {
            System.out.println("Faunus: A Library of Graph-Based Hadoop Tools");
            System.out.println("FaunusPipeline Usage:");
            System.out.println("  arg1: faunus configuration location (optional - defaults to bin/faunus.properties)");
            System.out.println("  arg2: faunus script: g.V.step().step()...");
            System.exit(-1);
        }

        final String script;
        final String file;
        final java.util.Properties properties = new java.util.Properties();
        if (args.length == 1) {
            script = args[0];
            file = "bin/faunus.properties";
        } else {
            file = args[0];
            script = args[1];
        }
        properties.load(new FileInputStream(file));


        final Configuration conf = new Configuration();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            conf.set(entry.getKey().toString(), entry.getValue().toString());
        }

        final FaunusPipeline faunusPipeline = new FaunusPipeline(script, conf);
        final GroovyScriptEngineImpl scriptEngine = new GroovyScriptEngineImpl();
        scriptEngine.eval("Vertex= " + Vertex.class.getName());
        scriptEngine.eval("Edge= " + Edge.class.getName());
        scriptEngine.eval("IN=" + Direction.class.getName() + ".IN");
        scriptEngine.eval("OUT=" + Direction.class.getName() + ".OUT");
        scriptEngine.eval("BOTH=" + Direction.class.getName() + ".BOTH");
        scriptEngine.eval("KEEP=" + Tokens.Action.class.getName() + ".KEEP");
        scriptEngine.eval("DROP=" + Tokens.Action.class.getName() + ".DROP");
        scriptEngine.eval("REVERSE=" + Tokens.Order.class.getName() + ".REVERSE");
        scriptEngine.eval("STANDARD=" + Tokens.Order.class.getName() + ".STANDARD");
        scriptEngine.eval("eq=" + Query.Compare.class.getName() + ".EQUAL");
        scriptEngine.eval("neq=" + Query.Compare.class.getName() + ".NOT_EQUAL");
        scriptEngine.eval("lt=" + Query.Compare.class.getName() + ".LESS_THAN");
        scriptEngine.eval("lte=" + Query.Compare.class.getName() + ".LESS_THAN_EQUAL");
        scriptEngine.eval("gt=" + Query.Compare.class.getName() + ".GREATER_THAN");
        scriptEngine.eval("gte=" + Query.Compare.class.getName() + ".GREATER_THAN_EQUAL");

        scriptEngine.put("g", faunusPipeline);
        FaunusPipeline pipeline = ((FaunusPipeline) scriptEngine.eval(script));
        FaunusRunner runner = pipeline.compiler;
        runner.completeSequence();
        int result = ToolRunner.run(runner, args);
        System.exit(result);
    }
}
