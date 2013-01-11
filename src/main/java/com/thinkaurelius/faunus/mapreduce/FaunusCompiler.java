package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.formats.BlueprintsGraphOutputMapReduce;
import com.thinkaurelius.faunus.formats.edgelist.EdgeListInputMapReduce;
import com.thinkaurelius.faunus.formats.titan.SchemaInferencerMapReduce;
import com.thinkaurelius.faunus.hdfs.GraphFilter;
import com.thinkaurelius.faunus.hdfs.NoSideEffectFilter;
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
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Query;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusCompiler extends Configured implements Tool {

    public static final String PATH_ENABLED = Tokens.makeNamespace(FaunusCompiler.class) + ".pathEnabled";
    public static final String TESTING = Tokens.makeNamespace(FaunusCompiler.class) + ".testing";

    protected final Logger logger = Logger.getLogger(FaunusCompiler.class);

    private FaunusGraph graph;

    private final List<Job> jobs = new ArrayList<Job>();

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

    public FaunusCompiler(final FaunusGraph graph) {
        this.graph = graph;
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

    ///////// SPECIFIC TO THE BLUEPRINTS OUTPUT FORMAT

    public void blueprintsGraphOutputMapReduce() throws IOException {
        this.mapSequenceClasses.add(BlueprintsGraphOutputMapReduce.Map.class);
        this.reduceClass = BlueprintsGraphOutputMapReduce.Reduce.class;
        this.setKeyValueClasses(LongWritable.class, Holder.class, NullWritable.class, FaunusVertex.class);
        this.completeSequence();
    }

    public void schemaInferenceMapReduce() throws IOException {
        this.mapSequenceClasses.add(SchemaInferencerMapReduce.Map.class);
        this.reduceClass = SchemaInferencerMapReduce.Reduce.class;
        this.setKeyValueClasses(LongWritable.class, FaunusVertex.class, NullWritable.class, FaunusVertex.class);
        this.completeSequence();
    }

    public void edgeListInputMapReduce() throws IOException {
        this.mapSequenceClasses.add(EdgeListInputMapReduce.Map.class);
        this.combinerClass = EdgeListInputMapReduce.Combiner.class;
        this.reduceClass = EdgeListInputMapReduce.Reduce.class;
        this.setKeyValueClasses(LongWritable.class, FaunusVertex.class, NullWritable.class, FaunusVertex.class);
        this.completeSequence();
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

    public void transformMap(final Class<? extends Element> klass, final String closure) throws IOException {
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

    public void propertyMap(final Class<? extends Element> klass, final String key, final Class<? extends WritableComparable> type) {
        this.mapSequenceConfiguration.setClass(PropertyMap.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceConfiguration.set(PropertyMap.KEY + "-" + this.mapSequenceClasses.size(), key);
        this.mapSequenceConfiguration.setClass(PropertyMap.TYPE + "-" + this.mapSequenceClasses.size(), type, WritableComparable.class);
        this.mapSequenceClasses.add(PropertyMap.Map.class);
        this.setKeyValueClasses(LongWritable.class, type);
    }

    public void propertyMapMap(final Class<? extends Element> klass) {
        this.mapSequenceConfiguration.setClass(PropertyMapMap.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceClasses.add(PropertyMapMap.Map.class);
        this.setKeyValueClasses(LongWritable.class, Text.class);
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
        this.combinerClass = BackFilterMapReduce.Combiner.class;
        this.reduceClass = BackFilterMapReduce.Reduce.class;

        this.setKeyValueClasses(LongWritable.class, Holder.class, NullWritable.class, FaunusVertex.class);
        this.completeSequence();
    }

    public void duplicateFilterMap(final Class<? extends Element> klass) {
        this.mapSequenceConfiguration.setClass(DuplicateFilterMap.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceClasses.add(DuplicateFilterMap.Map.class);
        this.setKeyValueClasses(NullWritable.class, FaunusVertex.class);
    }

    public void cyclePathFilterMap(final Class<? extends Element> klass) {
        this.mapSequenceConfiguration.setClass(CyclicPathFilterMap.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceClasses.add(CyclicPathFilterMap.Map.class);
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

    public void valueGroupCountMapReduce(final Class<? extends Element> klass, final String property, final Class<? extends WritableComparable> type) throws IOException {
        this.mapSequenceConfiguration.setClass(ValueGroupCountMapReduce.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceConfiguration.set(ValueGroupCountMapReduce.PROPERTY + "-" + this.mapSequenceClasses.size(), property);
        this.mapSequenceConfiguration.setClass(ValueGroupCountMapReduce.TYPE + "-" + this.mapSequenceClasses.size(), type, Writable.class);
        this.mapSequenceClasses.add(ValueGroupCountMapReduce.Map.class);
        this.combinerClass = ValueGroupCountMapReduce.Combiner.class;
        this.reduceClass = ValueGroupCountMapReduce.Reduce.class;
        this.setKeyValueClasses(type, LongWritable.class, type, LongWritable.class);
        this.completeSequence();
    }

    public void groupCountMapReduce(final Class<? extends Element> klass, final String keyClosure, final String valueClosure) throws IOException {
        this.mapSequenceConfiguration.setClass(GroupCountMapReduce.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        if (null != keyClosure)
            this.mapSequenceConfiguration.set(GroupCountMapReduce.KEY_CLOSURE + "-" + this.mapSequenceClasses.size(), keyClosure);
        if (null != valueClosure)
            this.mapSequenceConfiguration.set(GroupCountMapReduce.VALUE_CLOSURE + "-" + this.mapSequenceClasses.size(), valueClosure);
        this.mapSequenceClasses.add(GroupCountMapReduce.Map.class);
        this.combinerClass = GroupCountMapReduce.Combiner.class;
        this.reduceClass = GroupCountMapReduce.Reduce.class;
        this.setKeyValueClasses(Text.class, LongWritable.class, Text.class, LongWritable.class);
        this.completeSequence();
    }

    /////////////////// EXTRA

    public void countMapReduce(final Class<? extends Element> klass) throws IOException {
        this.mapSequenceConfiguration.setClass(CountMapReduce.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceClasses.add(CountMapReduce.Map.class);
        this.combinerClass = CountMapReduce.Combiner.class;
        this.reduceClass = CountMapReduce.Reduce.class;
        this.setKeyValueClasses(NullWritable.class, LongWritable.class, NullWritable.class, LongWritable.class);
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
            if (null != this.reduceClass) {
                job.setReducerClass(this.reduceClass);
                if (null != this.combinerClass)
                    job.setCombinerClass(this.combinerClass);
                // if there is a reduce task, compress the map output to limit network traffic
                job.getConfiguration().setBoolean("mapred.compress.map.output", true);
                job.getConfiguration().setClass("mapred.map.output.compression.codec", DefaultCodec.class, CompressionCodec.class);
            } else {
                job.setNumReduceTasks(0);
            }

            if (this.mapSequenceClasses.contains(BlueprintsGraphOutputMapReduce.Map.class)) {
                job.setMapSpeculativeExecution(false);
                job.setReduceSpeculativeExecution(false);
            }

            job.setMapOutputKeyClass(this.mapOutputKey);
            job.setMapOutputValueClass(this.mapOutputValue);
            job.setOutputKeyClass(this.outputKey);
            job.setOutputValueClass(this.outputValue);

            if (null != this.comparatorClass)
                job.setSortComparatorClass(this.comparatorClass);

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

        // TODO: rectify this hell
        final String hadoopFileJar;
        if (new File("target/" + Tokens.FAUNUS_JOB_JAR).exists())
            hadoopFileJar = "target/" + Tokens.FAUNUS_JOB_JAR;
        else if (new File("../../" + Tokens.FAUNUS_JOB_JAR).exists())
            // this path is here when running the .bat from mvn clean install
            hadoopFileJar = "../../" + Tokens.FAUNUS_JOB_JAR;
        else if (new File("lib/" + Tokens.FAUNUS_JOB_JAR).exists())
            hadoopFileJar = "lib/" + Tokens.FAUNUS_JOB_JAR;
        else if (new File("../lib/" + Tokens.FAUNUS_JOB_JAR).exists())
            hadoopFileJar = "../lib/" + Tokens.FAUNUS_JOB_JAR;
        else
            throw new IllegalStateException("The Faunus Hadoop job jar could not be found: " + Tokens.FAUNUS_JOB_JAR);

        if (this.pathEnabled)
            logger.warn("Path calculations are enabled for this Faunus job (space and time expensive)");

        for (final Job job : this.jobs) {
            job.getConfiguration().setBoolean(PATH_ENABLED, this.pathEnabled);
            job.getConfiguration().set("mapred.jar", hadoopFileJar);
        }

        final FileSystem hdfs = FileSystem.get(this.graph.getConfiguration());
        final String outputJobPrefix = this.graph.getOutputLocation().toString() + "/" + Tokens.JOB;
        hdfs.mkdirs(this.graph.getOutputLocation());

        //////// CHAINING JOBS TOGETHER

        if (FileInputFormat.class.isAssignableFrom(this.graph.getGraphInputFormat())) {
            FileInputFormat.setInputPaths(this.jobs.get(0), this.graph.getInputLocation());
            FileInputFormat.setInputPathFilter(this.jobs.get(0), NoSideEffectFilter.class);
        }

        for (int i = 0; i < this.jobs.size(); i++) {
            final Job job = this.jobs.get(i);
            final Path path = new Path(outputJobPrefix + "-" + i);
            FileOutputFormat.setOutputPath(job, path);

            if (i == 0) {
                job.setInputFormatClass(this.graph.getGraphInputFormat());
            } else {
                job.setInputFormatClass(INTERMEDIATE_INPUT_FORMAT);
                FileInputFormat.setInputPathFilter(job, GraphFilter.class);
                FileInputFormat.addInputPath(job, new Path(outputJobPrefix + "-" + (i - 1)));
            }

            if (i == this.jobs.size() - 1) {
                LazyOutputFormat.setOutputFormatClass(job, this.graph.getGraphOutputFormat());
                MultipleOutputs.addNamedOutput(job, Tokens.SIDEEFFECT, this.graph.getSideEffectOutputFormat(), job.getOutputKeyClass(), job.getOutputKeyClass());
                MultipleOutputs.addNamedOutput(job, Tokens.GRAPH, this.graph.getGraphOutputFormat(), NullWritable.class, FaunusVertex.class);
            } else {
                LazyOutputFormat.setOutputFormatClass(job, INTERMEDIATE_OUTPUT_FORMAT);
                MultipleOutputs.addNamedOutput(job, Tokens.SIDEEFFECT, this.graph.getSideEffectOutputFormat(), job.getOutputKeyClass(), job.getOutputKeyClass());
                MultipleOutputs.addNamedOutput(job, Tokens.GRAPH, INTERMEDIATE_OUTPUT_FORMAT, NullWritable.class, FaunusVertex.class);
            }
        }
    }

    public int run(final String[] args) throws Exception {
        String script = null;
        boolean showHeader = true;

        if (args.length == 2) {
            script = args[0];
            showHeader = Boolean.valueOf(args[1]);
        }

        final FileSystem hdfs = FileSystem.get(this.getConf());
        if (this.graph.getOutputLocationOverwrite() && hdfs.exists(this.graph.getOutputLocation())) {
            hdfs.delete(this.graph.getOutputLocation(), true);
        }

        if (showHeader) {
            logger.info("Faunus: Graph Analytics Engine");
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
        }

        if (null != script && !script.isEmpty())
            logger.info("Generating job chain: " + script);

        this.composeJobs();
        logger.info("Compiled to " + this.jobs.size() + " MapReduce job(s)");
        final String jobPath = this.graph.getOutputLocation().toString() + "/" + Tokens.JOB;
        for (int i = 0; i < this.jobs.size(); i++) {
            final Job job = this.jobs.get(i);
            logger.info("Executing job " + (i + 1) + " out of " + this.jobs.size() + ": " + job.getJobName());
            logger.info("Job data location: " + jobPath + "-" + i);
            boolean success = job.waitForCompletion(true);
            if (i > 0) {
                final Path path = new Path(jobPath + "-" + (i - 1));
                // delete previous intermediate graph data
                for (final FileStatus temp : hdfs.globStatus(new Path(path.toString() + "/" + Tokens.GRAPH + "*"))) {
                    hdfs.delete(temp.getPath(), true);
                }
                // delete previous intermediate graph data
                for (final FileStatus temp : hdfs.globStatus(new Path(path.toString() + "/" + Tokens.PART + "*"))) {
                    hdfs.delete(temp.getPath(), true);
                }
            }

            if (!success) {
                logger.error("Faunus job error -- remaining MapReduce jobs have been canceled");
                return -1;
            }
        }

        return 0;
    }
}