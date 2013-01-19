package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.MapReduceFormat;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.mapreduce.StepMapReduce;
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
import com.thinkaurelius.faunus.mapreduce.transform.PropertyMapMap;
import com.thinkaurelius.faunus.mapreduce.transform.TransformMap;
import com.thinkaurelius.faunus.mapreduce.transform.VertexMap;
import com.thinkaurelius.faunus.mapreduce.transform.VerticesEdgesMapReduce;
import com.thinkaurelius.faunus.mapreduce.transform.VerticesMap;
import com.thinkaurelius.faunus.mapreduce.transform.VerticesVerticesMapReduce;
import com.thinkaurelius.faunus.mapreduce.util.CountMapReduce;
import com.thinkaurelius.faunus.mapreduce.util.WritableComparators;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.pipes.util.structures.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
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

        if (MapReduceFormat.class.isAssignableFrom(this.graph.getGraphInputFormat())) {
            try {
                ((Class<? extends MapReduceFormat>) this.graph.getGraphInputFormat()).getConstructor().newInstance().addMapReduceJobs(this.compiler);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
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
        final Configuration configuration = new Configuration();
        configuration.setClass(StepMapReduce.CLASS, this.state.getElementType(), Element.class);
        configuration.set(StepMapReduce.MAP_CLOSURE, this.validateClosure(mapClosure));
        configuration.set(StepMapReduce.REDUCE_CLOSURE, this.validateClosure(reduceClosure));
        this.compiler.addMapReduce(StepMapReduce.Map.class,
                null,
                StepMapReduce.Reduce.class,
                key1, value1, key2, value2, configuration);
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
        this.compiler.addMap(IdentityMap.Map.class, NullWritable.class, FaunusVertex.class, null);
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
        final Configuration configuration = new Configuration();
        configuration.setClass(TransformMap.CLASS, this.state.getElementType(), Element.class);
        configuration.set(TransformMap.CLOSURE, this.validateClosure(closure));
        this.compiler.addMap(TransformMap.Map.class, NullWritable.class, FaunusVertex.class, configuration);
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
        final Configuration configuration = new Configuration();
        configuration.setBoolean(VerticesMap.PROCESS_EDGES, this.state.incrStep() != 0);
        this.compiler.addMap(VerticesMap.Map.class, NullWritable.class, FaunusVertex.class, configuration);
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
        final Configuration configuration = new Configuration();
        configuration.setBoolean(EdgesMap.PROCESS_VERTICES, this.state.incrStep() != 0);
        this.compiler.addMap(EdgesMap.Map.class, NullWritable.class, FaunusVertex.class, configuration);
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

        final String[] idStrings = new String[ids.length];
        for (int i = 0; i < ids.length; i++) {
            idStrings[i] = String.valueOf(ids[i]);
        }
        final Configuration configuration = new Configuration();
        configuration.setStrings(VertexMap.IDS, idStrings);

        this.compiler.addMap(VertexMap.Map.class, NullWritable.class, FaunusVertex.class, configuration);
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
        return this.inOutBoth(OUT, labels);
    }

    /**
     * Take incoming labeled edges to adjacent vertices.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline in(final String... labels) throws IOException {
        return this.inOutBoth(IN, labels);
    }

    /**
     * Take both incoming and outgoing labeled edges to adjacent vertices.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline both(final String... labels) throws IOException {
        return this.inOutBoth(BOTH, labels);
    }

    private FaunusPipeline inOutBoth(final Direction direction, final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.incrStep();
        if (this.state.atVertex()) {
            final Configuration configuration = new Configuration();
            configuration.set(VerticesVerticesMapReduce.DIRECTION, direction.name());
            configuration.setStrings(VerticesVerticesMapReduce.LABELS, labels);
            this.compiler.addMapReduce(VerticesVerticesMapReduce.Map.class,
                    VerticesVerticesMapReduce.Combiner.class,
                    VerticesVerticesMapReduce.Reduce.class,
                    LongWritable.Comparator.class,
                    LongWritable.class,
                    Holder.class,
                    NullWritable.class,
                    FaunusVertex.class, configuration);
            this.state.set(Vertex.class);
            makeMapReduceString(VerticesVerticesMapReduce.class, direction.name(), Arrays.asList(labels));
            return this;
        } else {
            throw new IllegalStateException("This step can not follow an edge-based step");
        }
    }

    /**
     * Take outgoing labeled edges to incident edges.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline outE(final String... labels) throws IOException {
        return this.inOutBothE(OUT, labels);
    }

    /**
     * Take incoming labeled edges to incident edges.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline inE(final String... labels) throws IOException {
        return this.inOutBothE(IN, labels);
    }

    /**
     * Take both incoming and outgoing labeled edges to incident edges.
     *
     * @param labels the labels of the edges to traverse over
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline bothE(final String... labels) throws IOException {
        return this.inOutBothE(BOTH, labels);
    }

    private FaunusPipeline inOutBothE(final Direction direction, final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.incrStep();
        if (this.state.atVertex()) {
            final Configuration configuration = new Configuration();
            configuration.set(VerticesEdgesMapReduce.DIRECTION, direction.name());
            configuration.setStrings(VerticesVerticesMapReduce.LABELS, labels);
            this.compiler.addMapReduce(VerticesVerticesMapReduce.Map.class,
                    VerticesVerticesMapReduce.Combiner.class,
                    VerticesVerticesMapReduce.Reduce.class,
                    LongWritable.Comparator.class,
                    LongWritable.class, Holder.class, NullWritable.class, FaunusVertex.class, configuration);
            this.state.set(Edge.class);
            makeMapReduceString(VerticesEdgesMapReduce.class, direction.name(), Arrays.asList(labels));
            return this;
        } else {
            throw new IllegalStateException("This step can not follow an edge-based step");
        }
    }

    /**
     * Go to the outgoing/tail vertex of the edge.
     *
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline outV() throws IOException {
        return this.inOutBothV(OUT);
    }

    /**
     * Go to the incoming/head vertex of the edge.
     *
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline inV() throws IOException {
        return this.inOutBothV(IN);
    }

    /**
     * Go to both the incoming/head and outgoing/tail vertices of the edge.
     *
     * @return the extended FaunusPipeline
     * @throws IOException
     */
    public FaunusPipeline bothV() throws IOException {
        return this.inOutBothV(BOTH);
    }

    private FaunusPipeline inOutBothV(final Direction direction, final String... labels) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        this.state.incrStep();
        if (!this.state.atVertex()) {
            final Configuration configuration = new Configuration();
            configuration.set(EdgesVerticesMap.DIRECTION, direction.name());
            this.compiler.addMap(EdgesVerticesMap.Map.class, NullWritable.class, FaunusVertex.class, configuration);
            this.state.set(Vertex.class);
            makeMapReduceString(EdgesVerticesMap.class, direction.name());
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
        final Configuration configuration = new Configuration();
        configuration.setClass(PropertyMapMap.CLASS, this.state.getElementType(), Element.class);
        this.compiler.addMap(PropertyMapMap.Map.class, LongWritable.class, Text.class, configuration);
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
     * @return the extended FaunusPipeline
     */
    public FaunusPipeline path() {
        this.state.checkLocked();
        this.state.checkProperty();

        final Configuration configuration = new Configuration();
        configuration.setClass(PathMap.CLASS, this.state.getElementType(), Element.class);
        this.compiler.addMap(PathMap.Map.class, NullWritable.class, Text.class, configuration);
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
            final Configuration configuration = new Configuration();
            configuration.setClass(OrderMapReduce.CLASS, this.state.getElementType(), Element.class);
            configuration.set(OrderMapReduce.KEY, pair.getA());
            configuration.setClass(OrderMapReduce.TYPE, pair.getB(), WritableComparable.class);
            configuration.set(OrderMapReduce.ELEMENT_KEY, elementKey);
            Class<? extends WritableComparator> comparatorClass = null;
            if (pair.getB().equals(LongWritable.class))
                comparatorClass = order.equals(Tokens.Order.INCREASING) ? LongWritable.Comparator.class : LongWritable.DecreasingComparator.class;
            else if (pair.getB().equals(IntWritable.class))
                comparatorClass = order.equals(Tokens.Order.INCREASING) ? IntWritable.Comparator.class : WritableComparators.DecreasingIntComparator.class;
            else if (pair.getB().equals(FloatWritable.class))
                comparatorClass = order.equals(Tokens.Order.INCREASING) ? FloatWritable.Comparator.class : WritableComparators.DecreasingFloatComparator.class;
            else if (pair.getB().equals(DoubleWritable.class))
                comparatorClass = order.equals(Tokens.Order.INCREASING) ? DoubleWritable.Comparator.class : WritableComparators.DecreasingDoubleComparator.class;
            else if (pair.getB().equals(Text.class))
                comparatorClass = order.equals(Tokens.Order.INCREASING) ? Text.Comparator.class : WritableComparators.DecreasingTextComparator.class;
            this.compiler.addMapReduce(OrderMapReduce.Map.class,
                    null,
                    OrderMapReduce.Reduce.class,
                    comparatorClass, pair.getB(), Text.class, Text.class, pair.getB(), configuration);
            makeMapReduceString(OrderMapReduce.class, order.name(), elementKey);
        } else {
            throw new IllegalArgumentException("There is no specified property to order on");
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

        final Configuration configuration = new Configuration();
        configuration.setClass(FilterMap.CLASS, this.state.getElementType(), Element.class);
        configuration.set(FilterMap.CLOSURE, this.validateClosure(closure));
        this.compiler.addMap(FilterMap.Map.class, NullWritable.class, FaunusVertex.class, configuration);
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

        final String[] valueStrings = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            valueStrings[i] = values[i].toString();
        }
        final Configuration configuration = new Configuration();
        configuration.setClass(PropertyFilterMap.CLASS, this.state.getElementType(), Element.class);
        configuration.set(PropertyFilterMap.KEY, key);
        configuration.set(PropertyFilterMap.COMPARE, compare.name());
        configuration.setStrings(PropertyFilterMap.VALUES, valueStrings);
        configuration.setBoolean(PropertyFilterMap.NULL_WILDCARD, false); // TODO: parameterize?
        if (values[0] instanceof String) {
            configuration.setClass(PropertyFilterMap.VALUE_CLASS, String.class, String.class);
        } else if (values[0] instanceof Boolean) {
            configuration.setClass(PropertyFilterMap.VALUE_CLASS, Boolean.class, Boolean.class);
        } else if (values[0] instanceof Number) {
            configuration.setClass(PropertyFilterMap.VALUE_CLASS, Number.class, Number.class);
        } else {
            throw new RuntimeException("Unknown value class: " + values[0].getClass().getName());
        }
        this.compiler.addMap(PropertyFilterMap.Map.class, NullWritable.class, FaunusVertex.class, configuration);
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

        final Configuration configuration = new Configuration();
        configuration.setClass(IntervalFilterMap.CLASS, this.state.getElementType(), Element.class);
        configuration.set(IntervalFilterMap.KEY, key);
        configuration.setBoolean(IntervalFilterMap.NULL_WILDCARD, false);  // TODO: Parameterize?
        if (startValue instanceof String) {
            configuration.set(IntervalFilterMap.VALUE_CLASS, String.class.getName());
            configuration.set(IntervalFilterMap.START_VALUE, (String) startValue);
            configuration.set(IntervalFilterMap.END_VALUE, (String) endValue);
        } else if (startValue instanceof Number) {
            configuration.set(IntervalFilterMap.VALUE_CLASS, Float.class.getName());
            configuration.setFloat(IntervalFilterMap.START_VALUE, ((Number) startValue).floatValue());
            configuration.setFloat(IntervalFilterMap.END_VALUE, ((Number) endValue).floatValue());
        } else if (startValue instanceof Boolean) {
            configuration.set(IntervalFilterMap.VALUE_CLASS, Boolean.class.getName());
            configuration.setBoolean(IntervalFilterMap.START_VALUE, (Boolean) startValue);
            configuration.setBoolean(IntervalFilterMap.END_VALUE, (Boolean) endValue);
        } else {
            throw new RuntimeException("Unknown value class: " + startValue.getClass().getName());
        }

        this.compiler.addMap(IntervalFilterMap.Map.class, NullWritable.class, FaunusVertex.class, configuration);
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

        final Configuration configuration = new Configuration();
        configuration.setClass(DuplicateFilterMap.CLASS, this.state.getElementType(), Element.class);
        this.compiler.addMap(DuplicateFilterMap.Map.class, NullWritable.class, FaunusVertex.class, configuration);
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

        final Configuration configuration = new Configuration();
        configuration.setInt(BackFilterMapReduce.STEP, this.state.getStep(step));
        configuration.setClass(BackFilterMapReduce.CLASS, this.state.getElementType(), Element.class);
        this.compiler.addMapReduce(BackFilterMapReduce.Map.class,
                BackFilterMapReduce.Combiner.class,
                BackFilterMapReduce.Reduce.class,
                LongWritable.class, Holder.class, NullWritable.class, FaunusVertex.class, configuration);
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

        final Configuration configuration = new Configuration();
        configuration.setClass(CyclicPathFilterMap.CLASS, this.state.getElementType(), Element.class);
        this.compiler.addMap(CyclicPathFilterMap.Map.class, NullWritable.class, FaunusVertex.class, configuration);
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

        final Configuration configuration = new Configuration();
        configuration.setClass(SideEffectMap.CLASS, this.state.getElementType(), Element.class);
        configuration.set(SideEffectMap.CLOSURE, this.validateClosure(closure));
        this.compiler.addMap(SideEffectMap.Map.class, NullWritable.class, FaunusVertex.class, configuration);

        makeMapReduceString(SideEffectMap.class);
        return this;
    }

    /**
     * Name a step in order to reference it later in the expression.
     *
     * @param name the string representation of the name
     * @return the extended FaunusPipeline
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
        return this.link(IN, step, label, mergeWeightKey);
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
        return this.link(IN, step, label, null);
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
        return link(OUT, step, label, mergeWeightKey);
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
        return this.link(OUT, step, label, null);
    }

    private FaunusPipeline link(final Direction direction, final String step, final String label, final String mergeWeightKey) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        final Configuration configuration = new Configuration();
        configuration.setInt(LinkMapReduce.STEP, this.state.getStep(step));
        configuration.set(LinkMapReduce.DIRECTION, direction.name());
        configuration.set(LinkMapReduce.LABEL, label);
        if (null == mergeWeightKey) {
            configuration.setBoolean(LinkMapReduce.MERGE_DUPLICATES, false);
            configuration.set(LinkMapReduce.MERGE_WEIGHT_KEY, LinkMapReduce.NO_WEIGHT_KEY);
        } else {
            configuration.setBoolean(LinkMapReduce.MERGE_DUPLICATES, true);
            configuration.set(LinkMapReduce.MERGE_WEIGHT_KEY, mergeWeightKey);
        }
        this.compiler.addMapReduce(LinkMapReduce.Map.class,
                LinkMapReduce.Combiner.class,
                LinkMapReduce.Reduce.class,
                LongWritable.Comparator.class,
                LongWritable.class, Holder.class, NullWritable.class, FaunusVertex.class, configuration);

        if (null != mergeWeightKey)
            makeMapReduceString(LinkMapReduce.class, direction.name(), step, label, mergeWeightKey);
        else
            makeMapReduceString(LinkMapReduce.class, direction.name(), step, label);
        this.compiler.setPathEnabled(true);
        return this;
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
            return this.groupCount(null, null);
        } else {
            final Configuration configuration = new Configuration();
            configuration.setClass(ValueGroupCountMapReduce.CLASS, this.state.getElementType(), Element.class);
            configuration.set(ValueGroupCountMapReduce.PROPERTY, pair.getA());
            configuration.setClass(ValueGroupCountMapReduce.TYPE, pair.getB(), Writable.class);
            this.compiler.addMapReduce(ValueGroupCountMapReduce.Map.class,
                    ValueGroupCountMapReduce.Combiner.class,
                    ValueGroupCountMapReduce.Reduce.class,
                    pair.getB(), LongWritable.class, pair.getB(), LongWritable.class, configuration);
            makeMapReduceString(ValueGroupCountMapReduce.class, pair.getA());
        }
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
        return this.groupCount(keyClosure, null);
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

        final Configuration configuration = new Configuration();
        configuration.setClass(GroupCountMapReduce.CLASS, this.state.getElementType(), Element.class);
        if (null != keyClosure)
            configuration.set(GroupCountMapReduce.KEY_CLOSURE, this.validateClosure(keyClosure));
        if (null != valueClosure)
            configuration.set(GroupCountMapReduce.VALUE_CLOSURE, this.validateClosure(valueClosure));
        this.compiler.addMapReduce(GroupCountMapReduce.Map.class,
                GroupCountMapReduce.Combiner.class,
                GroupCountMapReduce.Reduce.class,
                Text.class, LongWritable.class, Text.class, LongWritable.class, configuration);

        makeMapReduceString(GroupCountMapReduce.class);
        return this;
    }


    private FaunusPipeline commit(final Tokens.Action action) throws IOException {
        this.state.checkLocked();
        this.state.checkProperty();

        final Configuration configuration = new Configuration();
        if (this.state.atVertex()) {
            configuration.set(CommitVerticesMapReduce.ACTION, action.name());
            this.compiler.addMapReduce(CommitVerticesMapReduce.Map.class,
                    CommitVerticesMapReduce.Combiner.class,
                    CommitVerticesMapReduce.Reduce.class,
                    LongWritable.Comparator.class,
                    LongWritable.class, Holder.class, NullWritable.class, FaunusVertex.class, configuration);
            makeMapReduceString(CommitVerticesMapReduce.class, action.name());
        } else {
            configuration.set(CommitEdgesMap.ACTION, action.name());
            this.compiler.addMap(CommitEdgesMap.Map.class, NullWritable.class, FaunusVertex.class, configuration);
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
        Configuration configuration = new Configuration();
        configuration.setClass(CountMapReduce.CLASS, this.state.getElementType(), Element.class);
        this.compiler.addMapReduce(CountMapReduce.Map.class,
                CountMapReduce.Combiner.class,
                CountMapReduce.Reduce.class, NullWritable.class, LongWritable.class, NullWritable.class, LongWritable.class, configuration);

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

                final Configuration configuration = new Configuration();
                configuration.setClass(PropertyMap.CLASS, this.state.getElementType(), Element.class);
                configuration.set(PropertyMap.KEY, pair.getA());
                configuration.setClass(PropertyMap.TYPE, pair.getB(), WritableComparable.class);
                this.compiler.addMap(PropertyMap.Map.class, LongWritable.class, pair.getB(), configuration);
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
        if (MapReduceFormat.class.isAssignableFrom(this.graph.getGraphOutputFormat())) {
            this.state.checkLocked();
            ((Class<? extends MapReduceFormat>) this.graph.getGraphOutputFormat()).getConstructor().newInstance().addMapReduceJobs(this.compiler);
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
