package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.formats.graphson.GraphSONInputFormat;
import com.thinkaurelius.faunus.formats.rexster.RexsterInputFormat;
import com.thinkaurelius.faunus.formats.titan.TitanCassandraInputFormat;
import com.thinkaurelius.faunus.mapreduce.filter.BackFilterMapReduce;
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
import com.thinkaurelius.faunus.mapreduce.util.WritableComparators;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusCompiler extends Configured implements Tool {

    public static final String PATH_ENABLED = Tokens.makeNamespace(FaunusCompiler.class) + ".pathEnabled";

    protected final Logger logger = Logger.getLogger(FaunusCompiler.class);
    private final String jobScript;

    private FaunusGraph graph;
    private boolean derivationJob = true;

    private final List<Job> jobs = new ArrayList<Job>();
    private final List<Path> intermediateFiles = new ArrayList<Path>();

    private final Configuration mapSequenceConfiguration = new Configuration();
    private final List<Class<? extends Mapper>> mapSequenceClasses = new ArrayList<Class<? extends Mapper>>();
    private Class<? extends WritableComparable> mapOutputKey = NullWritable.class;
    private Class<? extends WritableComparable> mapOutputValue = NullWritable.class;
    private Class<? extends WritableComparable> outputKey = NullWritable.class;
    private Class<? extends WritableComparable> outputValue = NullWritable.class;

    private Class<? extends Reducer> combinerClass = null;
    private Class<? extends WritableComparator> comparatorClass = null;
    private Class<? extends Reducer> reduceClass = null;


    private static final Class<? extends InputFormat> INTERMEDIATE_INPUT_FORMAT = SequenceFileInputFormat.class;
    private static final Class<? extends OutputFormat> INTERMEDIATE_OUTPUT_FORMAT = SequenceFileOutputFormat.class;

    private boolean pathEnabled = false;

    public FaunusCompiler(final String jobScript, final FaunusGraph graph) {
        this.jobScript = jobScript;
        this.graph = graph;
    }
    
    public List<Job> getJobs() {
        return this.jobs;
    }

    private String toStringOfJob(final Class sequenceClass) {
        final List<String> list = new ArrayList<String>();
        for (final Class klass : this.mapSequenceClasses) {
            list.add(klass.getCanonicalName());
        }
        if (null != reduceClass) {
            list.add(this.reduceClass.getCanonicalName());
        }
        return sequenceClass.getSimpleName() + list.toString();
    }

    private String[] toStringMapSequenceClasses() {
        final List<String> list = new ArrayList<String>();
        for (final Class klass : this.mapSequenceClasses) {
            list.add(klass.getName());
        }
        return list.toArray(new String[list.size()]);
    }

    private void setKeyValueClasses(final Class<? extends WritableComparable> mapOutputKey,
                                    final Class<? extends WritableComparable> mapOutputValue,
                                    final Class<? extends WritableComparable> outputKey,
                                    final Class<? extends WritableComparable> outputValue) {

        this.mapOutputKey = mapOutputKey;
        this.mapOutputValue = mapOutputValue;
        this.outputKey = outputKey;
        this.outputValue = outputValue;
    }

    private void setKeyValueClasses(final Class<? extends WritableComparable> mapOutputKey,
                                    final Class<? extends WritableComparable> mapOutputValue) {

        this.mapOutputKey = mapOutputKey;
        this.mapOutputValue = mapOutputValue;
        this.outputKey = mapOutputKey;
        this.outputValue = mapOutputValue;
    }

    public void setPathEnabled(final boolean pathEnabled) {
        this.pathEnabled = pathEnabled;
    }

    ////////////// STEP

    public void stepMapReduce(final Class<? extends Element> klass, final String mapClosure, final String reduceClosure, final Class<? extends WritableComparable> key1, final Class<? extends WritableComparable> value1, final Class<? extends WritableComparable> key2, final Class<? extends WritableComparable> value2) throws IOException {
        this.mapSequenceConfiguration.setClass(StepMapReduce.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceConfiguration.set(StepMapReduce.MAP_CLOSURE + "-" + this.mapSequenceClasses.size(), mapClosure);
        this.mapSequenceConfiguration.set(StepMapReduce.REDUCE_CLOSURE, reduceClosure);
        this.mapSequenceClasses.add(StepMapReduce.Map.class);
        this.reduceClass = StepMapReduce.Reduce.class;
        this.setKeyValueClasses(key1, value1, key2, value2);
    }

    ///////////// TRANSFORMS

    public void transform(final Class<? extends Element> klass, final String closure) throws IOException {
        this.mapSequenceConfiguration.setClass(TransformMap.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceConfiguration.set(TransformMap.CLOSURE + "-" + this.mapSequenceClasses.size(), closure);
        this.mapSequenceClasses.add(TransformMap.Map.class);
        this.setKeyValueClasses(NullWritable.class, FaunusVertex.class);
    }

    public void _() {
        this.mapSequenceClasses.add(IdentityMap.Map.class);
        this.setKeyValueClasses(NullWritable.class, FaunusVertex.class);
    }

    public void vertexMap(final long... ids) {
        final String[] idStrings = new String[ids.length];
        for (int i = 0; i < ids.length; i++) {
            idStrings[i] = String.valueOf(ids[i]);
        }
        this.mapSequenceConfiguration.setStrings(VertexMap.IDS + "-" + this.mapSequenceClasses.size(), idStrings);
        this.mapSequenceClasses.add(VertexMap.Map.class);
        this.setKeyValueClasses(NullWritable.class, FaunusVertex.class);
    }

    public void verticesMap(final boolean processEdges) {
        this.mapSequenceConfiguration.setBoolean(VerticesMap.PROCESS_EDGES + "-" + this.mapSequenceClasses.size(), processEdges);
        this.mapSequenceClasses.add(VerticesMap.Map.class);
        this.setKeyValueClasses(NullWritable.class, FaunusVertex.class);
    }

    public void edgesMap(final boolean processVertices) {
        this.mapSequenceConfiguration.setBoolean(EdgesMap.PROCESS_VERTICES + "-" + this.mapSequenceClasses.size(), processVertices);
        this.mapSequenceClasses.add(EdgesMap.Map.class);
        this.setKeyValueClasses(NullWritable.class, FaunusVertex.class);
    }

    public void verticesVerticesMapReduce(final Direction direction, final String... labels) throws IOException {
        this.mapSequenceConfiguration.set(VerticesVerticesMapReduce.DIRECTION + "-" + this.mapSequenceClasses.size(), direction.name());
        this.mapSequenceConfiguration.setStrings(VerticesVerticesMapReduce.LABELS + "-" + this.mapSequenceClasses.size(), labels);
        this.mapSequenceClasses.add(VerticesVerticesMapReduce.Map.class);
        this.reduceClass = VerticesVerticesMapReduce.Reduce.class;
        this.comparatorClass = LongWritable.Comparator.class;
        this.setKeyValueClasses(LongWritable.class, Holder.class, NullWritable.class, FaunusVertex.class);
        this.completeSequence();
    }

    public void verticesEdgesMapReduce(final Direction direction, final String... labels) throws IOException {
        this.mapSequenceConfiguration.set(VerticesEdgesMapReduce.DIRECTION + "-" + this.mapSequenceClasses.size(), direction.name());
        this.mapSequenceConfiguration.setStrings(VerticesEdgesMapReduce.LABELS + "-" + this.mapSequenceClasses.size(), labels);
        this.mapSequenceClasses.add(VerticesEdgesMapReduce.Map.class);

        this.mapSequenceConfiguration.set(VerticesEdgesMapReduce.DIRECTION, direction.name()); // TODO: make more robust
        this.mapSequenceConfiguration.setStrings(VerticesEdgesMapReduce.LABELS, labels);
        this.reduceClass = VerticesEdgesMapReduce.Reduce.class;
        this.comparatorClass = LongWritable.Comparator.class;
        this.setKeyValueClasses(LongWritable.class, Holder.class, NullWritable.class, FaunusVertex.class);
        this.completeSequence();
    }

    public void edgesVerticesMap(final Direction direction) throws IOException {
        this.mapSequenceConfiguration.set(EdgesVerticesMap.DIRECTION + "-" + this.mapSequenceClasses.size(), direction.name());
        this.mapSequenceClasses.add(EdgesVerticesMap.Map.class);
        this.setKeyValueClasses(NullWritable.class, FaunusVertex.class);
    }

    public void propertyMap(final Class<? extends Element> klass, final String key, final Class<? extends WritableComparable> type) throws IOException {
        this.mapSequenceConfiguration.setClass(PropertyMap.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceConfiguration.set(PropertyMap.KEY + "-" + this.mapSequenceClasses.size(), key);
        this.mapSequenceConfiguration.setClass(PropertyMap.TYPE + "-" + this.mapSequenceClasses.size(), type, WritableComparable.class);
        this.mapSequenceClasses.add(PropertyMap.Map.class);
        this.setKeyValueClasses(LongWritable.class, type);
    }

    public void orderMapReduce(final Class<? extends Element> klass, final String elementKey, final String key, final Class<? extends WritableComparable> type, final Tokens.Order order) throws IOException {
        this.mapSequenceConfiguration.setClass(OrderMapReduce.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceConfiguration.set(OrderMapReduce.KEY + "-" + this.mapSequenceClasses.size(), key);
        this.mapSequenceConfiguration.setClass(OrderMapReduce.TYPE + "-" + this.mapSequenceClasses.size(), type, WritableComparable.class);
        this.mapSequenceConfiguration.set(OrderMapReduce.ELEMENT_KEY + "-" + this.mapSequenceClasses.size(), elementKey);

        if (type.equals(LongWritable.class))
            this.comparatorClass = order.equals(Tokens.Order.INCREASING) ? LongWritable.Comparator.class : LongWritable.DecreasingComparator.class;
        else if (type.equals(IntWritable.class))
            this.comparatorClass = order.equals(Tokens.Order.INCREASING) ? IntWritable.Comparator.class : WritableComparators.DecreasingIntComparator.class;
        else if (type.equals(FloatWritable.class))
            this.comparatorClass = order.equals(Tokens.Order.INCREASING) ? FloatWritable.Comparator.class : WritableComparators.DecreasingFloatComparator.class;
        else if (type.equals(DoubleWritable.class))
            this.comparatorClass = order.equals(Tokens.Order.INCREASING) ? DoubleWritable.Comparator.class : WritableComparators.DecreasingDoubleComparator.class;
        else if (type.equals(Text.class))
            this.comparatorClass = order.equals(Tokens.Order.INCREASING) ? Text.Comparator.class : WritableComparators.DecreasingTextComparator.class;

        this.mapSequenceClasses.add(OrderMapReduce.Map.class);
        this.reduceClass = OrderMapReduce.Reduce.class;
        this.setKeyValueClasses(type, Text.class, Text.class, type);
        this.completeSequence();
    }


    public void pathMap(final Class<? extends Element> klass) throws IOException {
        this.mapSequenceConfiguration.setClass(PathMap.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceClasses.add(PathMap.Map.class);
        this.setKeyValueClasses(NullWritable.class, Text.class);
    }

    ///////////// FILTERS

    public void filterMap(final Class<? extends Element> klass, final String closure) {
        if (!klass.equals(Vertex.class) && !klass.equals(Edge.class)) {
            throw new RuntimeException("Unsupported element class: " + klass.getName());
        }

        this.mapSequenceConfiguration.setClass(FilterMap.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceConfiguration.set(FilterMap.CLOSURE + "-" + this.mapSequenceClasses.size(), closure);
        this.mapSequenceClasses.add(FilterMap.Map.class);
        this.setKeyValueClasses(NullWritable.class, FaunusVertex.class);
    }

    public void propertyFilterMap(final Class<? extends Element> klass, final boolean nullIsWildcard, final String key, final Query.Compare compare, final Object... values) {
        final String[] valueStrings = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            valueStrings[i] = values[i].toString();
        }
        this.mapSequenceConfiguration.setClass(PropertyFilterMap.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceConfiguration.set(PropertyFilterMap.KEY + "-" + this.mapSequenceClasses.size(), key);
        this.mapSequenceConfiguration.set(PropertyFilterMap.COMPARE + "-" + this.mapSequenceClasses.size(), compare.name());
        this.mapSequenceConfiguration.setStrings(PropertyFilterMap.VALUES + "-" + this.mapSequenceClasses.size(), valueStrings);
        this.mapSequenceConfiguration.setBoolean(PropertyFilterMap.NULL_WILDCARD, nullIsWildcard);
        if (values[0] instanceof String) {
            this.mapSequenceConfiguration.setClass(PropertyFilterMap.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), String.class, String.class);
        } else if (values[0] instanceof Boolean) {
            this.mapSequenceConfiguration.setClass(PropertyFilterMap.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), Boolean.class, Boolean.class);
        } else if (values[0] instanceof Number) {
            this.mapSequenceConfiguration.setClass(PropertyFilterMap.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), Number.class, Number.class);
        } else {
            throw new RuntimeException("Unknown value class: " + values[0].getClass().getName());
        }
        this.mapSequenceClasses.add(PropertyFilterMap.Map.class);
        this.setKeyValueClasses(NullWritable.class, FaunusVertex.class);
    }

    public void intervalFilterMap(final Class<? extends Element> klass, final boolean nullIsWildcard, final String key, final Object startValue, final Object endValue) {
        this.mapSequenceConfiguration.setClass(IntervalFilterMap.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceConfiguration.set(IntervalFilterMap.KEY + "-" + this.mapSequenceClasses.size(), key);
        this.mapSequenceConfiguration.setBoolean(IntervalFilterMap.NULL_WILDCARD, nullIsWildcard);
        if (startValue instanceof String) {
            this.mapSequenceConfiguration.set(IntervalFilterMap.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), String.class.getName());
            this.mapSequenceConfiguration.set(IntervalFilterMap.START_VALUE + "-" + this.mapSequenceClasses.size(), (String) startValue);
            this.mapSequenceConfiguration.set(IntervalFilterMap.END_VALUE + "-" + this.mapSequenceClasses.size(), (String) endValue);
        } else if (startValue instanceof Number) {
            this.mapSequenceConfiguration.set(IntervalFilterMap.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), Float.class.getName());
            this.mapSequenceConfiguration.setFloat(IntervalFilterMap.START_VALUE + "-" + this.mapSequenceClasses.size(), ((Number) startValue).floatValue());
            this.mapSequenceConfiguration.setFloat(IntervalFilterMap.END_VALUE + "-" + this.mapSequenceClasses.size(), ((Number) endValue).floatValue());
        } else if (startValue instanceof Boolean) {
            this.mapSequenceConfiguration.set(IntervalFilterMap.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), Boolean.class.getName());
            this.mapSequenceConfiguration.setBoolean(IntervalFilterMap.START_VALUE + "-" + this.mapSequenceClasses.size(), (Boolean) startValue);
            this.mapSequenceConfiguration.setBoolean(IntervalFilterMap.END_VALUE + "-" + this.mapSequenceClasses.size(), (Boolean) endValue);
        } else {
            throw new RuntimeException("Unknown value class: " + startValue.getClass().getName());
        }

        this.mapSequenceClasses.add(IntervalFilterMap.Map.class);
        this.setKeyValueClasses(NullWritable.class, FaunusVertex.class);
    }

    public void backFilterMapReduce(final Class<? extends Element> klass, final int step) throws IOException {
        this.mapSequenceConfiguration.setInt(BackFilterMapReduce.STEP + "-" + this.mapSequenceClasses.size(), step);
        this.mapSequenceConfiguration.setClass(BackFilterMapReduce.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceClasses.add(BackFilterMapReduce.Map.class);
        this.reduceClass = BackFilterMapReduce.Reduce.class;

        this.setKeyValueClasses(LongWritable.class, Holder.class, NullWritable.class, FaunusVertex.class);
        this.completeSequence();
    }

    public void duplicateFilterMap(final Class<? extends Element> klass) {
        if (!klass.equals(Vertex.class) && !klass.equals(Edge.class)) {
            throw new RuntimeException("Unsupported element class: " + klass.getName());
        }

        this.mapSequenceConfiguration.setClass(DuplicateFilterMap.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceClasses.add(DuplicateFilterMap.Map.class);
        this.setKeyValueClasses(NullWritable.class, FaunusVertex.class);
    }

    ///////////// SIDE-EFFECTS

    public void sideEffect(final Class<? extends Element> klass, final String closure) {
        this.mapSequenceConfiguration.setClass(SideEffectMap.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceConfiguration.set(SideEffectMap.CLOSURE + "-" + this.mapSequenceClasses.size(), closure);
        this.mapSequenceClasses.add(SideEffectMap.Map.class);
        this.setKeyValueClasses(NullWritable.class, FaunusVertex.class);
    }


    public void linkMapReduce(final int step, final Direction direction, final String label, final String mergeWeightKey) throws IOException {
        this.mapSequenceConfiguration.setInt(LinkMapReduce.STEP + "-" + this.mapSequenceClasses.size(), step);
        this.mapSequenceConfiguration.set(LinkMapReduce.DIRECTION + "-" + this.mapSequenceClasses.size(), direction.name());
        this.mapSequenceConfiguration.set(LinkMapReduce.LABEL + "-" + this.mapSequenceClasses.size(), label);
        if (null == mergeWeightKey) {
            this.mapSequenceConfiguration.setBoolean(LinkMapReduce.MERGE_DUPLICATES + "-" + this.mapSequenceClasses.size(), false);
            this.mapSequenceConfiguration.set(LinkMapReduce.MERGE_WEIGHT_KEY + "-" + this.mapSequenceClasses.size(), LinkMapReduce.NO_WEIGHT_KEY);
        } else {
            this.mapSequenceConfiguration.setBoolean(LinkMapReduce.MERGE_DUPLICATES + "-" + this.mapSequenceClasses.size(), true);
            this.mapSequenceConfiguration.set(LinkMapReduce.MERGE_WEIGHT_KEY + "-" + this.mapSequenceClasses.size(), mergeWeightKey);
        }
        this.mapSequenceClasses.add(LinkMapReduce.Map.class);

        this.mapSequenceConfiguration.set(LinkMapReduce.DIRECTION, direction.name());  // TODO: make model more robust
        this.comparatorClass = LongWritable.Comparator.class;
        this.reduceClass = LinkMapReduce.Reduce.class;
        this.setKeyValueClasses(LongWritable.class, Holder.class, NullWritable.class, FaunusVertex.class);
        this.completeSequence();
    }

    public void commitEdgesMap(final Tokens.Action action) {
        this.mapSequenceConfiguration.set(CommitEdgesMap.ACTION + "-" + mapSequenceClasses.size(), action.name());
        this.mapSequenceClasses.add(CommitEdgesMap.Map.class);
        this.setKeyValueClasses(NullWritable.class, FaunusVertex.class);
    }

    public void commitVerticesMapReduce(final Tokens.Action action) throws IOException {
        this.mapSequenceConfiguration.set(CommitVerticesMapReduce.ACTION + "-" + mapSequenceClasses.size(), action.name());
        this.mapSequenceClasses.add(CommitVerticesMapReduce.Map.class);
        this.reduceClass = CommitVerticesMapReduce.Reduce.class;
        this.comparatorClass = LongWritable.Comparator.class;
        this.setKeyValueClasses(LongWritable.class, Holder.class, NullWritable.class, FaunusVertex.class);
        this.completeSequence();
    }

    public void valueDistribution(final Class<? extends Element> klass, final String property, final Class<? extends WritableComparable> type) throws IOException {
        this.mapSequenceConfiguration.setClass(ValueGroupCountMapReduce.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceConfiguration.set(ValueGroupCountMapReduce.PROPERTY + "-" + this.mapSequenceClasses.size(), property);
        this.mapSequenceConfiguration.setClass(ValueGroupCountMapReduce.TYPE + "-" + this.mapSequenceClasses.size(), type, Writable.class);
        this.mapSequenceClasses.add(ValueGroupCountMapReduce.Map.class);
        this.combinerClass = ValueGroupCountMapReduce.Reduce.class;
        this.reduceClass = ValueGroupCountMapReduce.Reduce.class;
        this.setKeyValueClasses(type, LongWritable.class, type, LongWritable.class);
        this.completeSequence();
    }

    public void groupCountMapReduce(final Class<? extends Element> klass, final String keyClosure, final String valueClosure) throws IOException {
        this.mapSequenceConfiguration.setClass(GroupCountMapReduce.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceConfiguration.set(GroupCountMapReduce.KEY_CLOSURE + "-" + this.mapSequenceClasses.size(), keyClosure);
        this.mapSequenceConfiguration.set(GroupCountMapReduce.VALUE_CLOSURE + "-" + this.mapSequenceClasses.size(), valueClosure);
        this.mapSequenceClasses.add(GroupCountMapReduce.Map.class);
        this.combinerClass = GroupCountMapReduce.Reduce.class;
        this.reduceClass = GroupCountMapReduce.Reduce.class;
        this.setKeyValueClasses(Text.class, LongWritable.class, Text.class, LongWritable.class);
        this.completeSequence();
    }

    /////////////////// EXTRA

    public void countMapReduce(final Class<? extends Element> klass) throws IOException {
        this.mapSequenceConfiguration.setClass(CountMapReduce.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceClasses.add(CountMapReduce.Map.class);
        this.comparatorClass = IntWritable.Comparator.class;
        this.reduceClass = CountMapReduce.Reduce.class;
        this.setKeyValueClasses(IntWritable.class, LongWritable.class, IntWritable.class, Text.class);
        this.completeSequence();
    }

    /////////////////////////////// UTILITIES


    public void completeSequence() throws IOException {
        if (this.mapSequenceClasses.size() > 0) {
            this.mapSequenceConfiguration.setStrings(MapSequence.MAP_CLASSES, toStringMapSequenceClasses());
            final Job job = new Job(this.mapSequenceConfiguration, this.toStringOfJob(MapSequence.class));

            // copy over any global configuration from faunus.properties and -D CLI
            for (final Map.Entry<String, String> entry : this.graph.getConfiguration()) {
                job.getConfiguration().set(entry.getKey(), entry.getValue());
            }

            job.setJarByClass(FaunusCompiler.class);
            job.setMapperClass(MapSequence.Map.class);
            if (this.reduceClass != null) {
                job.setReducerClass(this.reduceClass);
                if (this.combinerClass != null)
                    job.setCombinerClass(this.combinerClass);
            } else {
                job.setNumReduceTasks(0);
            }

            job.setMapOutputKeyClass(this.mapOutputKey);
            job.setMapOutputValueClass(this.mapOutputValue);
            job.setOutputKeyClass(this.outputKey);
            job.setOutputValueClass(this.outputValue);

            if (null != this.comparatorClass)
                job.setSortComparatorClass(this.comparatorClass);

            if (!(this.outputKey.equals(NullWritable.class) && this.outputValue.equals(FaunusVertex.class)))
                this.derivationJob = false;

            this.jobs.add(job);

            this.mapSequenceConfiguration.clear();
            this.mapSequenceClasses.clear();
            this.combinerClass = null;
            this.reduceClass = null;
            this.comparatorClass = null;
        }
    }

    public void composeJobs() throws IOException {
        if (this.jobs.size() == 0) {
            return;
        }

        for (final Job job : this.jobs) {
            job.getConfiguration().setBoolean(PATH_ENABLED, this.pathEnabled);
        }

        final FileSystem hdfs = FileSystem.get(this.graph.getConfiguration());
        try {
            final Job startJob = this.jobs.get(0);
            startJob.setInputFormatClass(this.graph.getGraphInputFormat());
            // configure input location
            if (this.graph.getGraphInputFormat().equals(GraphSONInputFormat.class) || this.graph.getGraphInputFormat().equals(SequenceFileInputFormat.class)) {
                FileInputFormat.setInputPaths(startJob, this.graph.getInputLocation());
            } else if (this.graph.getGraphInputFormat().equals(RexsterInputFormat.class)) {
                /* do nothing */
            } else if (this.graph.getGraphInputFormat().equals(TitanCassandraInputFormat.class)) {
                ConfigHelper.setInputColumnFamily(this.jobs.get(0).getConfiguration(), ConfigHelper.getInputKeyspace(this.graph.getConfiguration()), "edgestore");

                final SlicePredicate predicate = new SlicePredicate();
                final SliceRange sliceRange = new SliceRange();
                sliceRange.setStart(new byte[0]);
                sliceRange.setFinish(new byte[0]);
                predicate.setSlice_range(sliceRange);

                ConfigHelper.setInputSlicePredicate(this.jobs.get(0).getConfiguration(), predicate);
            } else
                throw new IOException(this.graph.getGraphInputFormat().getName() + " is not a supported input format");


            if (this.jobs.size() > 1) {
                final Path path = new Path(UUID.randomUUID().toString());
                FileOutputFormat.setOutputPath(startJob, path);
                this.intermediateFiles.add(path);
                startJob.setOutputFormatClass(INTERMEDIATE_OUTPUT_FORMAT);
            } else {
                FileOutputFormat.setOutputPath(startJob, this.graph.getOutputLocation());
                startJob.setOutputFormatClass(this.derivationJob ? this.graph.getGraphOutputFormat() : this.graph.getStatisticsOutputFormat());
            }

            if (this.jobs.size() > 2) {
                for (int i = 1; i < this.jobs.size() - 1; i++) {
                    final Job midJob = this.jobs.get(i);
                    midJob.setInputFormatClass(INTERMEDIATE_INPUT_FORMAT);
                    midJob.setOutputFormatClass(INTERMEDIATE_OUTPUT_FORMAT);
                    FileInputFormat.setInputPaths(midJob, this.intermediateFiles.get(this.intermediateFiles.size() - 1));
                    final Path path = new Path(UUID.randomUUID().toString());
                    FileOutputFormat.setOutputPath(midJob, path);
                    this.intermediateFiles.add(path);
                }
            }
            if (this.jobs.size() > 1) {
                final Job endJob = this.jobs.get(this.jobs.size() - 1);
                endJob.setInputFormatClass(INTERMEDIATE_INPUT_FORMAT);
                endJob.setOutputFormatClass(this.derivationJob ? this.graph.getGraphOutputFormat() : this.graph.getStatisticsOutputFormat());
                FileInputFormat.setInputPaths(endJob, this.intermediateFiles.get(this.intermediateFiles.size() - 1));
                FileOutputFormat.setOutputPath(endJob, this.graph.getOutputLocation());
            }
        } catch (IOException e) {
            for (final Path path : this.intermediateFiles) {
                try {
                    if (hdfs.exists(path)) {
                        hdfs.delete(path, true);
                    }
                } catch (IOException e1) {
                }
            }
            throw e;
        }
    }

    public int run(String[] args) throws Exception {
        if (this.graph.getOutputLocationOverwrite()) {
            final FileSystem hdfs = FileSystem.get(this.getConf());
            if (hdfs.exists(this.graph.getOutputLocation())) {
                hdfs.delete(this.graph.getOutputLocation(), true);
            }
        }
        logger.info("Faunus: A Library of Hadoop-Based Graph Tools");
        logger.info("        ,");
        logger.info("    ,   |\\ ,__");
        logger.info("    |\\   \\/   `\\");
        logger.info("    \\ `-.:.     `\\");
        logger.info("     `-.__ `\\/\\/\\|");
        logger.info("        / `'/ () \\");
        logger.info("      .'   /\\     )");
        logger.info("   .-'  .'| \\  \\__");
        logger.info(" .'  __(  \\  '`(()");
        logger.info("/_.'`  `.  |    )(");
        logger.info("         \\ |");
        logger.info("          |/");
        logger.info("Generating job chain: " + this.jobScript);
        this.composeJobs();
        logger.info("Compiled to " + this.jobs.size() + " MapReduce job(s)");

        for (int i = 0; i < this.jobs.size(); i++) {
            final Job job = this.jobs.get(i);
            logger.info("Executing job " + (i + 1) + " out of " + this.jobs.size() + ": " + job.getJobName());
            boolean success = job.waitForCompletion(true);
            if (i > 0 && this.intermediateFiles.size() > 0) {
                final FileSystem hdfs = FileSystem.get(job.getConfiguration());
                final Path path = this.intermediateFiles.remove(0);
                if (hdfs.exists(path))
                    hdfs.delete(path, true);
            }
            if (!success) {
                logger.error("There was an error in the Faunus job -- remaining MapReduce jobs have been canceled");
                break;
            }
        }
        return 0;
    }


}
