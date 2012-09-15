package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.mapreduce.filter.BackFilterMapReduce;
import com.thinkaurelius.faunus.mapreduce.filter.CyclicPathFilterMap;
import com.thinkaurelius.faunus.mapreduce.filter.DuplicateFilterMap;
import com.thinkaurelius.faunus.mapreduce.filter.FilterMap;
import com.thinkaurelius.faunus.mapreduce.filter.IntervalFilterMap;
import com.thinkaurelius.faunus.mapreduce.filter.PropertyFilterMap;
import com.thinkaurelius.faunus.mapreduce.sideeffect.CommitEdgesMap;
import com.thinkaurelius.faunus.mapreduce.sideeffect.CommitVerticesMapReduce;
import com.thinkaurelius.faunus.mapreduce.sideeffect.GroupCountMapReduce;
import com.thinkaurelius.faunus.mapreduce.sideeffect.LinkMapReduce;
import com.thinkaurelius.faunus.mapreduce.sideeffect.SideEffectMap;
import com.thinkaurelius.faunus.mapreduce.sideeffect.ValueGroupCountMapReduce;
import com.thinkaurelius.faunus.mapreduce.transform.EdgesMap;
import com.thinkaurelius.faunus.mapreduce.transform.EdgesVerticesMap;
import com.thinkaurelius.faunus.mapreduce.transform.IdentityMap;
import com.thinkaurelius.faunus.mapreduce.transform.OrderMapReduce;
import com.thinkaurelius.faunus.mapreduce.transform.PathMap;
import com.thinkaurelius.faunus.mapreduce.transform.PropertyMap;
import com.thinkaurelius.faunus.mapreduce.transform.TransformMap;
import com.thinkaurelius.faunus.mapreduce.transform.VertexMap;
import com.thinkaurelius.faunus.mapreduce.transform.VerticesEdgesMapReduce;
import com.thinkaurelius.faunus.mapreduce.transform.VerticesMap;
import com.thinkaurelius.faunus.mapreduce.transform.VerticesVerticesMapReduce;
import com.thinkaurelius.faunus.mapreduce.util.CountMapReduce;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.pipes.util.structures.Pair;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.BOTH;
import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusPipeline {

    public static final String PIPELINE_IS_LOCKED = "No more steps are possible as pipeline is locked";

    protected final FaunusCompiler compiler;
    protected final FaunusGraph graph;
    protected final State state;
    protected static final ScriptEngine engine = new GroovyScriptEngineImpl();

    protected final List<String> stringRepresentation = new ArrayList<String>();

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

    private Query.Compare convert(final com.tinkerpop.gremlin.Tokens.T compare) {
        if (compare.equals(com.tinkerpop.gremlin.Tokens.T.eq))
            return Query.Compare.EQUAL;
        else if (compare.equals(com.tinkerpop.gremlin.Tokens.T.neq))
            return Query.Compare.NOT_EQUAL;
        else if (compare.equals(com.tinkerpop.gremlin.Tokens.T.gt))
            return Query.Compare.GREATER_THAN;
        else if (compare.equals(com.tinkerpop.gremlin.Tokens.T.gte))
            return Query.Compare.GREATER_THAN_EQUAL;
        else if (compare.equals(com.tinkerpop.gremlin.Tokens.T.lt))
            return Query.Compare.LESS_THAN;
        else
            return Query.Compare.LESS_THAN_EQUAL;
    }

    private Tokens.Order convert(final Tokens.F order) {
        if (order.equals(Tokens.F.decr))
            return Tokens.Order.DECREASING;
        else
            return Tokens.Order.INCREASING;

    }

    protected class State {
        private Class<? extends Element> elementType;
        private String property;
        private Class<? extends WritableComparable> propertyType;
        private int step = -1;
        private boolean locked = false;
        private Map<String, Integer> namedSteps = new HashMap<String, Integer>();

        public State set(Class<? extends Element> elementType) {
            if (!elementType.equals(Vertex.class) && !elementType.equals(Edge.class))
                throw new IllegalArgumentException("The element class type must be either Vertex or Edge");

            this.elementType = elementType;
            return this;
        }

        public Class<? extends Element> getElementType() {
            return this.elementType;
        }

        public boolean atVertex() {
            if (null == this.elementType)
                throw new IllegalStateException("No element type can be inferred: start vertex (or edge) set must be defined");
            return this.elementType.equals(Vertex.class);
        }

        public State setProperty(final String property, final Class type) {
            this.property = property;
            this.propertyType = convertJavaToHadoop(type);
            return this;
        }

        public Pair<String, Class<? extends WritableComparable>> popProperty() {
            if (null == this.property)
                return null;
            Pair<String, Class<? extends WritableComparable>> pair = new Pair<String, Class<? extends WritableComparable>>(this.property, this.propertyType);
            this.property = null;
            this.propertyType = null;
            return pair;
        }

        public int incrStep() {
            return ++this.step;
        }

        public int getStep() {
            return this.step;
        }

        public void checkLocked() {
            if (this.locked) throw new IllegalStateException(PIPELINE_IS_LOCKED);
        }

        public boolean isLocked() {
            return this.locked;
        }

        public void lock() {
            this.locked = true;
        }

        public void addStep(final String name) {
            if (this.step == -1)
                throw new IllegalArgumentException("There is no previous step to name");

            this.namedSteps.put(name, this.step);
        }

        public int getStep(final String name) {
            final Integer i = this.namedSteps.get(name);
            if (null == i)
                throw new IllegalArgumentException("There is no step identified by: " + name);
            else
                return i;
        }
    }


    ////////////////////////////////
    ////////////////////////////////
    ////////////////////////////////

    public FaunusPipeline(final FaunusGraph graph) {
        this.graph = graph;
        this.compiler = new FaunusCompiler(this.graph);
        this.state = new State();
    }

    ///////// STEP

    public FaunusPipeline step(final String mapClosure, final String reduceClosure,
                               final Class<? extends WritableComparable> key1,
                               final Class<? extends WritableComparable> value1,
                               final Class<? extends WritableComparable> key2,
                               final Class<? extends WritableComparable> value2) throws IOException {
        this.state.checkLocked();
        this.compiler.stepMapReduce(this.state.getElementType(), this.validateClosure(mapClosure), this.validateClosure(reduceClosure),
                convertJavaToHadoop(key1), convertJavaToHadoop(value1),
                convertJavaToHadoop(key2), convertJavaToHadoop(value2));
        this.state.lock();
        return this;
    }


    //////// TRANSFORMS

    public FaunusPipeline _() {
        this.state.checkLocked();
        this.compiler._();
        makeMapReduceString(IdentityMap.class);
        return this;
    }

    public FaunusPipeline transform(final String closure) throws IOException {
        this.state.checkLocked();
        this.compiler.transform(this.state.getElementType(), this.validateClosure(closure));
        this.state.lock();
        makeMapReduceString(TransformMap.class);
        return this;
    }

    public FaunusPipeline V() {
        this.state.checkLocked();
        this.state.set(Vertex.class);
        if (this.state.incrStep() == 0)
            this.compiler.verticesMap(false);
        else
            this.compiler.verticesMap(true);

        makeMapReduceString(VerticesMap.class);
        return this;
    }

    public FaunusPipeline E() {
        this.state.checkLocked();
        this.state.set(Edge.class);
        if (this.state.incrStep() == 0)
            this.compiler.edgesMap(false);
        else
            this.compiler.edgesMap(true);

        makeMapReduceString(EdgesMap.class);
        return this;
    }

    public FaunusPipeline v(final long... ids) {
        this.state.checkLocked();
        this.state.set(Vertex.class);
        this.state.incrStep();
        this.compiler.vertexMap(ids);

        makeMapReduceString(VertexMap.class);
        return this;
    }

    public FaunusPipeline out(final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.incrStep();
        if (this.state.atVertex()) {
            this.compiler.verticesVerticesMapReduce(OUT, labels);
            makeMapReduceString(VerticesVerticesMapReduce.class, OUT.name(), Arrays.asList(labels));
            return this;
        } else
            throw new IllegalStateException("This step can not follow an edge-based step");
    }

    public FaunusPipeline in(final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.incrStep();
        if (this.state.atVertex()) {
            this.compiler.verticesVerticesMapReduce(IN, labels);
            makeMapReduceString(VerticesVerticesMapReduce.class, IN.name(), Arrays.asList(labels));
            return this;
        } else
            throw new IllegalStateException("This step can not follow an edge-based step");
    }

    public FaunusPipeline both(final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.incrStep();
        if (this.state.atVertex()) {
            this.compiler.verticesVerticesMapReduce(BOTH, labels);
            makeMapReduceString(VerticesVerticesMapReduce.class, BOTH.name(), Arrays.asList(labels));
            return this;
        } else
            throw new IllegalStateException("This step can not follow an edge-based step");
    }

    public FaunusPipeline outE(final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.incrStep();
        if (this.state.atVertex()) {
            this.compiler.verticesEdgesMapReduce(OUT, labels);
            this.state.set(Edge.class);
            makeMapReduceString(VerticesEdgesMapReduce.class, OUT.name(), Arrays.asList(labels));
            return this;
        } else
            throw new IllegalStateException("This step can not follow an edge-based step");
    }

    public FaunusPipeline inE(final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.incrStep();
        if (this.state.atVertex()) {
            this.compiler.verticesEdgesMapReduce(IN, labels);
            this.state.set(Edge.class);
            makeMapReduceString(VerticesEdgesMapReduce.class, IN.name(), Arrays.asList(labels));
            return this;
        } else
            throw new IllegalStateException("This step can not follow an edge-based step");
    }

    public FaunusPipeline bothE(final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.incrStep();
        if (this.state.atVertex()) {
            this.compiler.verticesEdgesMapReduce(BOTH, labels);
            this.state.set(Edge.class);
            makeMapReduceString(VerticesEdgesMapReduce.class, BOTH.name(), Arrays.asList(labels));
            return this;
        } else
            throw new IllegalStateException("This step can not follow an edge-based step");
    }

    public FaunusPipeline outV() throws IOException {
        this.state.checkLocked();
        this.state.incrStep();
        if (!this.state.atVertex()) {
            this.compiler.edgesVerticesMap(OUT);
            this.state.set(Vertex.class);
            makeMapReduceString(EdgesVerticesMap.class, OUT.name());
            return this;
        } else
            throw new IllegalStateException("This step can not follow a vertex-based step");
    }

    public FaunusPipeline inV() throws IOException {
        this.state.checkLocked();
        this.state.incrStep();
        if (!this.state.atVertex()) {
            this.compiler.edgesVerticesMap(IN);
            this.state.set(Vertex.class);
            makeMapReduceString(EdgesVerticesMap.class, IN.name());
            return this;
        } else
            throw new IllegalStateException("This step can not follow a vertex-based step");
    }

    public FaunusPipeline bothV() throws IOException {
        this.state.checkLocked();
        this.state.incrStep();
        if (!this.state.atVertex()) {
            this.compiler.edgesVerticesMap(BOTH);
            this.state.set(Vertex.class);
            makeMapReduceString(EdgesVerticesMap.class, BOTH.name());
            return this;
        } else
            throw new IllegalStateException("This step can not follow a vertex-based step");
    }

    public FaunusPipeline property(final String key, final Class type) {
        this.state.checkLocked();
        this.state.setProperty(key, type);
        return this;
    }

    public FaunusPipeline property(final String key) {
        return this.property(key, String.class);
    }

    public FaunusPipeline map() {
        this.state.checkLocked();
        this.compiler.propertyMapMap(this.state.getElementType());
        makeMapReduceString(PropertyMap.class);
        this.state.lock();
        return this;
    }

    public FaunusPipeline label() {
        this.state.checkLocked();
        if (!this.state.atVertex()) {
            this.property(Tokens.LABEL, String.class);
            return this;
        } else
            throw new IllegalStateException("This step can not follow a vertex-based step");
    }

    public FaunusPipeline path() throws IOException {
        this.state.checkLocked();
        this.compiler.pathMap(this.state.getElementType());
        this.compiler.setPathEnabled(true);
        this.state.lock();
        makeMapReduceString(PathMap.class);
        return this;
    }

    public FaunusPipeline order(final Tokens.Order order, final String elementKey) throws IOException {
        this.state.checkLocked();
        final Pair<String, Class<? extends WritableComparable>> pair = this.state.popProperty();
        if (null != pair) {
            this.compiler.orderMapReduce(this.state.getElementType(), elementKey, pair.getA(), pair.getB(), order);
            makeMapReduceString(OrderMapReduce.class, order.name(), elementKey);
        } else {
            throw new IllegalArgumentException("There is no specified property to sort on");
        }
        this.state.lock();
        return this;
    }

    public FaunusPipeline order(final Tokens.Order order) throws IOException {
        return this.order(order, Tokens.ID);
    }

    public FaunusPipeline order(final Tokens.F order, final String elementKey) throws IOException {
        return this.order(convert(order), elementKey);
    }

    public FaunusPipeline order(final Tokens.F order) throws IOException {
        return this.order(convert(order));
    }


    //////// FILTERS

    public FaunusPipeline filter(final String closure) {
        this.state.checkLocked();
        this.compiler.filterMap(this.state.getElementType(), this.validateClosure(closure));
        makeMapReduceString(FilterMap.class);
        return this;
    }

    public FaunusPipeline has(final String key, final com.tinkerpop.gremlin.Tokens.T compare, final Object... values) {
        return this.has(key, convert(compare), values);
    }

    public FaunusPipeline hasNot(final String key, final com.tinkerpop.gremlin.Tokens.T compare, final Object... values) {
        return this.hasNot(key, convert(compare), values);
    }

    public FaunusPipeline has(final String key, final Query.Compare compare, final Object... values) {
        this.state.checkLocked();
        this.compiler.propertyFilterMap(this.state.getElementType(), false, key, compare, values);
        makeMapReduceString(PropertyFilterMap.class, compare.name(), Arrays.asList(values));
        return this;
    }

    public FaunusPipeline hasNot(final String key, final Query.Compare compare, final Object... values) {
        this.state.checkLocked();
        return this.has(key, this.opposite(compare), values);
    }

    public FaunusPipeline has(final String key, final Object... values) {
        this.state.checkLocked();
        return this.has(key, Query.Compare.EQUAL, values);
    }

    public FaunusPipeline hasNot(final String key, final Object... values) {
        this.state.checkLocked();
        return this.has(key, Query.Compare.NOT_EQUAL, values);
    }

    public FaunusPipeline interval(final String key, final Object startValue, final Object endValue) {
        this.state.checkLocked();
        this.compiler.intervalFilterMap(this.state.getElementType(), false, key, startValue, endValue);
        makeMapReduceString(IntervalFilterMap.class, key, startValue, endValue);
        return this;
    }

    public FaunusPipeline dedup() {
        this.state.checkLocked();
        this.compiler.duplicateFilterMap(this.state.getElementType());
        makeMapReduceString(DuplicateFilterMap.class);
        return this;
    }


    public FaunusPipeline back(final String step) throws IOException {
        this.state.checkLocked();
        this.compiler.backFilterMapReduce(this.state.getElementType(), this.state.getStep(step));
        this.compiler.setPathEnabled(true);
        makeMapReduceString(BackFilterMapReduce.class, step);
        return this;
    }

    /*public FaunusPipeline back(final int numberOfSteps) throws IOException {
        this.state.checkLocked();
        this.compiler.backFilterMapReduce(this.state.getElementType(), this.state.getStep() - numberOfSteps);
        this.compiler.setPathEnabled(true);
        makeMapReduceString(BackFilterMapReduce.class, numberOfSteps);
        return this;
    }*/

    public FaunusPipeline simplePath() {
        this.state.checkLocked();
        this.compiler.cyclePathFilterMap(this.state.getElementType());
        this.compiler.setPathEnabled(true);
        makeMapReduceString(CyclicPathFilterMap.class);
        return this;
    }

    //////// SIDEEFFECTS

    public FaunusPipeline sideEffect(final String closure) {
        this.state.checkLocked();
        this.compiler.sideEffect(this.state.getElementType(), this.validateClosure(closure));
        makeMapReduceString(SideEffectMap.class);
        return this;
    }

    public FaunusPipeline as(final String name) {
        this.state.checkLocked();
        this.state.addStep(name);

        final String string = "As(" + name + "," + this.stringRepresentation.get(this.state.getStep(name)) + ")";
        this.stringRepresentation.set(this.state.getStep(name), string);
        return this;
    }

    public FaunusPipeline linkIn(final String step, final String label, final String mergeWeightKey) throws IOException {
        this.state.checkLocked();
        this.compiler.linkMapReduce(this.state.getStep(step), IN, label, mergeWeightKey);
        this.compiler.setPathEnabled(true);
        makeMapReduceString(LinkMapReduce.class, IN.name(), step, label, mergeWeightKey);
        return this;
    }

    public FaunusPipeline linkIn(final String step, final String label) throws IOException {
        return this.linkIn(step, label, null);
    }

    public FaunusPipeline linkOut(final String step, final String label, final String mergeWeightKey) throws IOException {
        this.state.checkLocked();
        this.compiler.linkMapReduce(this.state.getStep(step), OUT, label, mergeWeightKey);
        this.compiler.setPathEnabled(true);
        makeMapReduceString(LinkMapReduce.class, OUT.name(), step, label, mergeWeightKey);
        return this;
    }

    public FaunusPipeline linkOut(final String step, final String label) throws IOException {
        return this.linkOut(step, label, null);
    }

    public FaunusPipeline groupCount() throws IOException {
        this.state.checkLocked();
        final Pair<String, Class<? extends WritableComparable>> pair = this.state.popProperty();
        if (null == pair)
            return this.groupCount("{it -> it}");
        else {
            this.compiler.valueDistribution(this.state.getElementType(), pair.getA(), pair.getB());
            makeMapReduceString(ValueGroupCountMapReduce.class, pair.getA());
        }
        return this;
    }

    public FaunusPipeline groupCount(final String keyClosure, final String valueClosure) throws IOException {
        this.state.checkLocked();
        this.compiler.groupCountMapReduce(this.state.getElementType(), this.validateClosure(keyClosure), this.validateClosure(valueClosure));
        makeMapReduceString(GroupCountMapReduce.class);
        return this;
    }

    public FaunusPipeline groupCount(final String keyClosure) throws IOException {
        this.state.checkLocked();
        this.compiler.groupCountMapReduce(this.state.getElementType(), this.validateClosure(keyClosure), this.validateClosure("{it -> 1}"));
        makeMapReduceString(GroupCountMapReduce.class);
        return this;
    }

    private FaunusPipeline commit(final Tokens.Action action) throws IOException {
        this.state.checkLocked();
        if (this.state.atVertex()) {
            this.compiler.commitVerticesMapReduce(action);
            makeMapReduceString(CommitVerticesMapReduce.class, action.name());
        } else {
            this.compiler.commitEdgesMap(action);
            makeMapReduceString(CommitEdgesMap.class, action.name());
        }
        return this;
    }

    public FaunusPipeline drop() throws IOException {
        return this.commit(Tokens.Action.DROP);
    }

    public FaunusPipeline keep() throws IOException {
        return this.commit(Tokens.Action.KEEP);
    }

    /////////////// UTILITIES

    public FaunusPipeline enablePath() {
        this.compiler.setPathEnabled(true);
        return this;
    }

    public FaunusPipeline count() throws IOException {
        this.state.checkLocked();
        this.compiler.countMapReduce(this.state.getElementType());
        makeMapReduceString(CountMapReduce.class);
        this.state.lock();
        return this;
    }

    public String toString() {
        return this.stringRepresentation.toString();
    }

    private FaunusPipeline done() throws IOException {
        if (!this.state.isLocked()) {
            final Pair<String, Class<? extends WritableComparable>> pair = this.state.popProperty();
            if (null != pair) {
                this.compiler.propertyMap(this.state.getElementType(), pair.getA(), pair.getB());
                makeMapReduceString(PropertyMap.class, pair.getA());
            }
            this.state.lock();
        }
        this.compiler.completeSequence();
        return this;
    }

    public void submit() throws Exception {
        submit(Tokens.EMPTY_STRING, false);
    }

    public void submit(final String script, final Boolean showHeader) throws Exception {
        this.done();
        ToolRunner.run(this.compiler, new String[]{script, showHeader.toString()});
    }

    private String validateClosure(String closure) {
        try {
            engine.eval(closure);
            return closure;
        } catch (ScriptException e) {
            closure = closure.trim();
            closure = closure.replaceFirst("\\{", "{it->");
            try {
                engine.eval(closure);
                return closure;
            } catch (ScriptException e1) {
            }
            throw new IllegalArgumentException("The provided closure is in error: " + e.getMessage(), e);
        }
    }

    private void makeMapReduceString(final Class klass, final Object... arguments) {
        String result = klass.getSimpleName();
        if (arguments.length > 0) {
            result = result + "(";
            for (final Object arg : arguments) {
                result = result + arg + ",";
            }
            result = result.substring(0, result.length() - 1) + ")";
        }
        this.stringRepresentation.add(result);
    }

    private Class<? extends WritableComparable> convertJavaToHadoop(final Class klass) {
        if (klass.equals(String.class)) {
            return Text.class;
        } else if (klass.equals(Integer.class)) {
            return IntWritable.class;
        } else if (klass.equals(Double.class)) {
            return DoubleWritable.class;
        } else if (klass.equals(Long.class)) {
            return LongWritable.class;
        } else if (klass.equals(Float.class)) {
            return FloatWritable.class;
        } else if (klass.equals(Boolean.class)) {
            return BooleanWritable.class;
        } else {
            throw new IllegalArgumentException("The provided class is not supported: " + klass.getSimpleName());
        }
    }
}
