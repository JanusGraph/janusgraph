package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.BlueprintsGraphOutputMapReduce;
import com.thinkaurelius.faunus.formats.titan.TitanOutputFormat;
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

import static com.tinkerpop.blueprints.Direction.*;


/**
 * A FaunusPipeline is used to construct a Gremlin expression which, in turn, is ultimately represented as a series
 * of MapReduce jobs in Hadoop.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusPipeline {

    protected static final ScriptEngine engine = new GroovyScriptEngineImpl();
    public static final String PIPELINE_IS_LOCKED = "No more steps are possible as pipeline is locked";

    protected final FaunusCompiler compiler;
    protected final FaunusGraph graph;
    protected final State state;

    protected final List<String> stringRepresentation = new ArrayList<String>();

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
                throw new IllegalStateException("No element type can be inferred: start vertices (or edges) set must be defined");
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

        public void checkProperty() {
            if (this.property != null) throw new IllegalStateException("This step can not follow a property reference");
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

    /**
     * Construct a FaunusPipeline
     *
     * @param graph the FaunusGraph that is the source of the traversal
     */
    public FaunusPipeline(final FaunusGraph graph) {
        this.graph = graph;
        this.compiler = new FaunusCompiler(this.graph);
        this.state = new State();
    }

    ///////// STEP

    /**
     * An arbitrary map/reduce computation (the most generic step possible)
     *
     * @param mapClosure    the map function
     * @param reduceClosure the reduce function
     * @param key1          the map output key class
     * @param value1        the map output value class
     * @param key2          the reduce output key class
     * @param value2        the reduce output value class
     * @return the extended FaunusPipeline
     * @throws IOException
     */
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

    /**
     * The identity step does not alter the graph in anyway.
     * It has the benefit of emitting various usefual counters.
     *
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline _() {
        this.state.checkLocked();
        this.compiler._();
        makeMapReduceString(IdentityMap.class);
        return this;
    }

    /**
     * Apply the provided closure to the current element and emit the result.
     *
     * @param closure the closure to apply to the element
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline transform(final String closure) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();
        this.compiler.transformMap(this.state.getElementType(), this.validateClosure(closure));
        this.state.lock();
        makeMapReduceString(TransformMap.class);
        return this;
    }

    /**
     * Start a traversal at all vertices in the graph.
     *
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline V() {
        this.state.checkLocked();
        this.state.checkProperty();
        this.state.set(Vertex.class);
        if (this.state.incrStep() == 0)
            this.compiler.verticesMap(false);
        else
            this.compiler.verticesMap(true);

        makeMapReduceString(VerticesMap.class);
        return this;
    }

    /**
     * Start a traversal at all edges in the graph.
     *
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline E() {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.set(Edge.class);
        if (this.state.incrStep() == 0)
            this.compiler.edgesMap(false);
        else
            this.compiler.edgesMap(true);

        makeMapReduceString(EdgesMap.class);
        return this;
    }

    /**
     * Start a traversal at the vertices identified by the provided ids.
     *
     * @param ids the ids of the vertices to start the traversal from
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline v(final long... ids) {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.set(Vertex.class);
        this.state.incrStep();
        this.compiler.vertexMap(ids);

        makeMapReduceString(VertexMap.class);
        return this;
    }

    /**
     * Take outgoing labeled edges to adjacent vertices.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline out(final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.incrStep();
        if (this.state.atVertex()) {
            this.compiler.verticesVerticesMapReduce(OUT, labels);
            makeMapReduceString(VerticesVerticesMapReduce.class, OUT.name(), Arrays.asList(labels));
            return this;
        } else
            throw new IllegalStateException("This step can not follow an edge-based step");
    }

    /**
     * Take incoming labeled edges to adjacent vertices.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline in(final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.incrStep();
        if (this.state.atVertex()) {
            this.compiler.verticesVerticesMapReduce(IN, labels);
            makeMapReduceString(VerticesVerticesMapReduce.class, IN.name(), Arrays.asList(labels));
            return this;
        } else
            throw new IllegalStateException("This step can not follow an edge-based step");
    }

    /**
     * Take both incoming and outgoing labeled edges to adjacent vertices.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline both(final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.incrStep();
        if (this.state.atVertex()) {
            this.compiler.verticesVerticesMapReduce(BOTH, labels);
            makeMapReduceString(VerticesVerticesMapReduce.class, BOTH.name(), Arrays.asList(labels));
            return this;
        } else
            throw new IllegalStateException("This step can not follow an edge-based step");
    }

    /**
     * Take outgoing labeled edges to incident edges.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline outE(final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.incrStep();
        if (this.state.atVertex()) {
            this.compiler.verticesEdgesMapReduce(OUT, labels);
            this.state.set(Edge.class);
            makeMapReduceString(VerticesEdgesMapReduce.class, OUT.name(), Arrays.asList(labels));
            return this;
        } else
            throw new IllegalStateException("This step can not follow an edge-based step");
    }

    /**
     * Take incoming labeled edges to incident edges.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline inE(final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.incrStep();
        if (this.state.atVertex()) {
            this.compiler.verticesEdgesMapReduce(IN, labels);
            this.state.set(Edge.class);
            makeMapReduceString(VerticesEdgesMapReduce.class, IN.name(), Arrays.asList(labels));
            return this;
        } else
            throw new IllegalStateException("This step can not follow an edge-based step");
    }

    /**
     * Take both incoming and outgoing labeled edges to incident edges.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline bothE(final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.incrStep();
        if (this.state.atVertex()) {
            this.compiler.verticesEdgesMapReduce(BOTH, labels);
            this.state.set(Edge.class);
            makeMapReduceString(VerticesEdgesMapReduce.class, BOTH.name(), Arrays.asList(labels));
            return this;
        } else
            throw new IllegalStateException("This step can not follow an edge-based step");
    }

    /**
     * Go to the outgoing/tail vertex of the edge.
     *
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline outV() throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.incrStep();
        if (!this.state.atVertex()) {
            this.compiler.edgesVerticesMap(OUT);
            this.state.set(Vertex.class);
            makeMapReduceString(EdgesVerticesMap.class, OUT.name());
            return this;
        } else
            throw new IllegalStateException("This step can not follow a vertex-based step");
    }

    /**
     * Go to the incoming/head vertex of the edge.
     *
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline inV() throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.incrStep();
        if (!this.state.atVertex()) {
            this.compiler.edgesVerticesMap(IN);
            this.state.set(Vertex.class);
            makeMapReduceString(EdgesVerticesMap.class, IN.name());
            return this;
        } else
            throw new IllegalStateException("This step can not follow a vertex-based step");
    }

    /**
     * Go to both the incoming/head and outgoing/tail vertices of the edge.
     *
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline bothV() throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.incrStep();
        if (!this.state.atVertex()) {
            this.compiler.edgesVerticesMap(BOTH);
            this.state.set(Vertex.class);
            makeMapReduceString(EdgesVerticesMap.class, BOTH.name());
            return this;
        } else
            throw new IllegalStateException("This step can not follow a vertex-based step");
    }

    /**
     * Emit the property value of an element.
     *
     * @param key  the key identifying the property
     * @param type the class of the property value (so Hadoop can intelligently handle the result)
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline property(final String key, final Class type) {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.setProperty(key, type);
        return this;
    }

    /**
     * Emit the property value of an element.
     *
     * @param key the key identifying the property
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline property(final String key) {
        return this.property(key, String.class);
    }

    /**
     * Emit a string representation of the property map.
     *
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline map() {
        this.state.checkLocked();
        this.state.checkProperty();

        this.compiler.propertyMapMap(this.state.getElementType());
        makeMapReduceString(PropertyMap.class);
        this.state.lock();
        return this;
    }

    /**
     * Emit the label of the current edge.
     *
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline label() {
        this.state.checkLocked();
        this.state.checkProperty();

        if (!this.state.atVertex()) {
            this.property(Tokens.LABEL, String.class);
            return this;
        } else
            throw new IllegalStateException("This step can not follow a vertex-based step");
    }

    /**
     * Emit the path taken from start to current element.
     *
     * @return
     * @throws IOException
     */
    public FaunusPipeline path() throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.compiler.pathMap(this.state.getElementType());
        this.compiler.setPathEnabled(true);
        this.state.lock();
        makeMapReduceString(PathMap.class);
        return this;
    }

    /**
     * Order the previous property value results and emit them with another element property value.
     * It is important to emit the previous property with a provided type else it is ordered lexigraphically.
     *
     * @param order      increasing and descending order
     * @param elementKey the key of the element to associate it with
     * @return the extended FaunusPipeline
     * @throws IOException
     */
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

    /**
     * Order the previous property value results.
     *
     * @param order increasing and descending order
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline order(final Tokens.Order order) throws IOException {
        return this.order(order, Tokens.ID);
    }

    /**
     * Order the previous property value results and emit them with another element property value.
     * It is important to emit the previous property with a provided type else it is ordered lexigraphically.
     *
     * @param order      increasing and descending order
     * @param elementKey the key of the element to associate it with
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline order(final Tokens.F order, final String elementKey) throws IOException {
        return this.order(convert(order), elementKey);
    }

    /**
     * Order the previous property value results.
     *
     * @param order increasing and descending order
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline order(final Tokens.F order) throws IOException {
        return this.order(convert(order));
    }


    //////// FILTERS

    /**
     * Emit or deny the current element based upon the provided boolean-based closure.
     *
     * @param closure return true to emit and false to remove.
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline filter(final String closure) {
        this.state.checkLocked();
        this.state.checkProperty();

        this.compiler.filterMap(this.state.getElementType(), this.validateClosure(closure));
        makeMapReduceString(FilterMap.class);
        return this;
    }

    /**
     * Emit the current element if it has a property value comparable to the provided values.
     *
     * @param key     the property key of the element
     * @param compare the comparator
     * @param values  the values to compare against where only one needs to succeed (or'd)
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline has(final String key, final com.tinkerpop.gremlin.Tokens.T compare, final Object... values) {
        return this.has(key, convert(compare), values);
    }

    /**
     * Emit the current element if it does not have a property value comparable to the provided values.
     *
     * @param key     the property key of the element
     * @param compare the comparator (will be not'd)
     * @param values  the values to compare against where only one needs to succeed (or'd)
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline hasNot(final String key, final com.tinkerpop.gremlin.Tokens.T compare, final Object... values) {
        return this.hasNot(key, convert(compare), values);
    }

    /**
     * Emit the current element if it has a property value comparable to the provided values.
     *
     * @param key     the property key of the element
     * @param compare the comparator
     * @param values  the values to compare against where only one needs to succeed (or'd)
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline has(final String key, final Query.Compare compare, final Object... values) {
        this.state.checkLocked();
        this.state.checkProperty();

        this.compiler.propertyFilterMap(this.state.getElementType(), false, key, compare, values);
        makeMapReduceString(PropertyFilterMap.class, compare.name(), Arrays.asList(values));
        return this;
    }

    /**
     * Emit the current element if it does not have a property value comparable to the provided values.
     *
     * @param key     the property key of the element
     * @param compare the comparator (will be not'd)
     * @param values  the values to compare against where only one needs to succeed (or'd)
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline hasNot(final String key, final Query.Compare compare, final Object... values) {
        return this.has(key, compare.opposite(), values);
    }

    /**
     * Emit the current element it has a property value equal to the provided values.
     *
     * @param key    the property key of the element
     * @param values the values to compare against where only one needs to succeed (or'd)
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline has(final String key, final Object... values) {
        return this.has(key, Query.Compare.EQUAL, values);
    }

    /**
     * Emit the current element it does not have a property value equal to the provided values.
     *
     * @param key    the property key of the element
     * @param values the values to compare against where only one needs to succeed (or'd)
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline hasNot(final String key, final Object... values) {
        return this.has(key, Query.Compare.NOT_EQUAL, values);
    }

    /**
     * Emit the current element it has a property value equal within the provided range.
     *
     * @param key        the property key of the element
     * @param startValue the start of the range (inclusive)
     * @param endValue   the end of the range (exclusive)
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline interval(final String key, final Object startValue, final Object endValue) {
        this.state.checkLocked();
        this.state.checkProperty();

        this.compiler.intervalFilterMap(this.state.getElementType(), false, key, startValue, endValue);
        makeMapReduceString(IntervalFilterMap.class, key, startValue, endValue);
        return this;
    }

    /**
     * Remove any duplicate traversers at a single element.
     *
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline dedup() {
        this.state.checkLocked();
        this.state.checkProperty();

        this.compiler.duplicateFilterMap(this.state.getElementType());
        makeMapReduceString(DuplicateFilterMap.class);
        return this;
    }

    /**
     * Go back to an element a named step ago.
     * Currently only backing up to vertices is supported.
     *
     * @param step the name of the step to back up to
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline back(final String step) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

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

    /**
     * Emit the element only if it was arrived at via a path that does not have cycles in it.
     *
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline simplePath() {
        this.state.checkLocked();
        this.state.checkProperty();

        this.compiler.cyclePathFilterMap(this.state.getElementType());
        this.compiler.setPathEnabled(true);
        makeMapReduceString(CyclicPathFilterMap.class);
        return this;
    }

    //////// SIDEEFFECTS

    /**
     * Emit the element, but compute some sideeffect in the process.
     * For example, mutate the properties of the element.
     *
     * @param closure the sideeffect closure whose results are ignored.
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline sideEffect(final String closure) {
        this.state.checkLocked();
        this.state.checkProperty();

        this.compiler.sideEffect(this.state.getElementType(), this.validateClosure(closure));
        makeMapReduceString(SideEffectMap.class);
        return this;
    }

    /**
     * Name a step in order to reference it later in the expression.
     *
     * @param name the string representation of the name
     * @return the extended FaunusPipeline¬
     */
    public FaunusPipeline as(final String name) {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.addStep(name);

        final String string = "As(" + name + "," + this.stringRepresentation.get(this.state.getStep(name)) + ")";
        this.stringRepresentation.set(this.state.getStep(name), string);
        return this;
    }

    /**
     * Have the elements for the named step previous project an edge to the current vertex with provided label.
     * If a merge weight key is provided, then count the number of duplicate edges between the same two vertices and add a weight.
     * No weight key is specified by "_" and then all duplicates are merged, but no weight is added to the resultant edge.
     *
     * @param step           the name of the step where the source vertices were
     * @param label          the label of the edge to project
     * @param mergeWeightKey the property key to use for weight
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline linkIn(final String step, final String label, final String mergeWeightKey) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.compiler.linkMapReduce(this.state.getStep(step), IN, label, mergeWeightKey);
        this.compiler.setPathEnabled(true);
        makeMapReduceString(LinkMapReduce.class, IN.name(), step, label, mergeWeightKey);
        return this;
    }

    /**
     * Have the elements for the named step previous project an edge to the current vertex with provided label.
     *
     * @param step  the name of the step where the source vertices were
     * @param label the label of the edge to project
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline linkIn(final String step, final String label) throws IOException {
        return this.linkIn(step, label, null);
    }

    /**
     * Have the elements for the named step previous project an edge from the current vertex with provided label.
     * If a merge weight key is provided, then count the number of duplicate edges between the same two vertices and add a weight.
     * No weight key is specified by "_" and then all duplicates are merged, but no weight is added to the resultant edge.
     *
     * @param step           the name of the step where the source vertices were
     * @param label          the label of the edge to project
     * @param mergeWeightKey the property key to use for weight
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline linkOut(final String step, final String label, final String mergeWeightKey) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.compiler.linkMapReduce(this.state.getStep(step), OUT, label, mergeWeightKey);
        this.compiler.setPathEnabled(true);
        makeMapReduceString(LinkMapReduce.class, OUT.name(), step, label, mergeWeightKey);
        return this;
    }

    /**
     * Have the elements for the named step previous project an edge from the current vertex with provided label.
     *
     * @param step  the name of the step where the source vertices were
     * @param label the label of the edge to project
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline linkOut(final String step, final String label) throws IOException {
        return this.linkOut(step, label, null);
    }

    /**
     * Count the number of times the previous element (or property) has been traversed to.
     * The results are stored in the jobs sideeffect file in HDFS.
     *
     * @return the extended FaunusPipeline.
     * @throws IOException
     */
    public FaunusPipeline groupCount() throws IOException {
        this.state.checkLocked();
        final Pair<String, Class<? extends WritableComparable>> pair = this.state.popProperty();
        if (null == pair) {
            this.compiler.groupCountMapReduce(this.state.getElementType(), null, null);
            makeMapReduceString(GroupCountMapReduce.class);
        } else {
            this.compiler.valueGroupCountMapReduce(this.state.getElementType(), pair.getA(), pair.getB());
            makeMapReduceString(ValueGroupCountMapReduce.class, pair.getA());
        }
        return this;
    }

    /**
     * Apply the provided closure to the incoming element to determine the grouping key.
     * Then apply the value closure to the current element to determine the count increment.
     * The results are stored in the jobs sideeffect file in HDFS.
     *
     * @return the extended FaunusPipeline.
     * @throws IOException
     */
    public FaunusPipeline groupCount(final String keyClosure, final String valueClosure) throws IOException {
        this.state.checkLocked();
        this.compiler.groupCountMapReduce(this.state.getElementType(), this.validateClosure(keyClosure), this.validateClosure(valueClosure));
        makeMapReduceString(GroupCountMapReduce.class);
        return this;
    }

    /**
     * Apply the provided closure to the incoming element to determine the grouping key.
     * The value of the count is incremented by 1
     * The results are stored in the jobs sideeffect file in HDFS.
     *
     * @return the extended FaunusPipeline.
     * @throws IOException
     */
    public FaunusPipeline groupCount(final String keyClosure) throws IOException {
        this.state.checkLocked();
        this.compiler.groupCountMapReduce(this.state.getElementType(), this.validateClosure(keyClosure), null);
        makeMapReduceString(GroupCountMapReduce.class);
        return this;
    }

    private FaunusPipeline commit(final Tokens.Action action) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        if (this.state.atVertex()) {
            this.compiler.commitVerticesMapReduce(action);
            makeMapReduceString(CommitVerticesMapReduce.class, action.name());
        } else {
            this.compiler.commitEdgesMap(action);
            makeMapReduceString(CommitEdgesMap.class, action.name());
        }
        return this;
    }

    /**
     * Drop all the elements of respective type at the current step. Keep all others.
     *
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline drop() throws IOException {
        return this.commit(Tokens.Action.DROP);
    }

    /**
     * Keep all the elements of the respetive type at the current step. Drop all others.
     *
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline keep() throws IOException {
        return this.commit(Tokens.Action.KEEP);
    }

    /////////////// UTILITIES

    /**
     * By default, paths are disabled. Computing paths is very expensive in terms of both space and time.
     * When a path-based step is added to the pipeline, this method is automatically called.
     *
     * @return the FaunusPipeline with path cacluations enabled
     */
    public FaunusPipeline enablePath() {
        this.compiler.setPathEnabled(true);
        return this;
    }

    /**
     * Count the number of traversers currently in the graph
     *
     * @return the count
     * @throws IOException
     */
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

    /**
     * Submit the FaunusPipeline to the Hadoop cluster.
     *
     * @throws Exception
     */
    public void submit() throws Exception {
        submit(Tokens.EMPTY_STRING, false);
    }

    /**
     * Submit the FaunusPipeline to the Hadoop cluster and ensure that a header is emitted in the logs.
     *
     * @param script     the Gremlin script
     * @param showHeader the Faunus header
     * @throws Exception
     */
    public void submit(final String script, final Boolean showHeader) throws Exception {
        if (TitanOutputFormat.class.isAssignableFrom(this.graph.getGraphOutputFormat())) {
            this.state.checkLocked();
            if (this.graph.getConfiguration().getBoolean(TitanOutputFormat.TITAN_GRAPH_OUTPUT_INFER_SCHEMA, true))
                this.compiler.schemaInferenceMapReduce();
            this.compiler.blueprintsGraphOutputMapReduce();
            makeMapReduceString(BlueprintsGraphOutputMapReduce.class);
        }
        this.done();
        ToolRunner.run(this.compiler, new String[]{script, showHeader.toString()});
    }

    /**
     * Get a reference to the graph currently being used in this FaunusPipeline.
     *
     * @return
     */
    public FaunusGraph getGraph() {
        return this.graph;
    }

    private String validateClosure(String closure) {
        //if (closure == null)
        //    return null;

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
