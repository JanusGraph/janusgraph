package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusConfiguration;
import com.thinkaurelius.faunus.FaunusPipeline;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.formats.graphson.GraphSONInputFormat;
import com.thinkaurelius.faunus.formats.rexster.RexsterInputFormat;
import com.thinkaurelius.faunus.formats.titan.TitanCassandraInputFormat;
import com.thinkaurelius.faunus.mapreduce.filter.BackFilterMapReduce;
import com.thinkaurelius.faunus.mapreduce.filter.FilterMap;
import com.thinkaurelius.faunus.mapreduce.filter.IntervalFilterMap;
import com.thinkaurelius.faunus.mapreduce.filter.PropertyFilterMap;
import com.thinkaurelius.faunus.mapreduce.sideeffect.CommitEdgesMap;
import com.thinkaurelius.faunus.mapreduce.sideeffect.CommitVerticesMapReduce;
import com.thinkaurelius.faunus.mapreduce.sideeffect.GroupCountMapReduce;
import com.thinkaurelius.faunus.mapreduce.sideeffect.LinkMapReduce;
import com.thinkaurelius.faunus.mapreduce.sideeffect.ValueGroupCountMapReduce;
import com.thinkaurelius.faunus.mapreduce.transform.EdgesMap;
import com.thinkaurelius.faunus.mapreduce.transform.EdgesVerticesMap;
import com.thinkaurelius.faunus.mapreduce.transform.IdentityMap;
import com.thinkaurelius.faunus.mapreduce.transform.PathMap;
import com.thinkaurelius.faunus.mapreduce.transform.TransformMap;
import com.thinkaurelius.faunus.mapreduce.transform.VertexMap;
import com.thinkaurelius.faunus.mapreduce.transform.VerticesEdgesMapReduce;
import com.thinkaurelius.faunus.mapreduce.transform.VerticesMap;
import com.thinkaurelius.faunus.mapreduce.transform.VerticesVerticesMapReduce;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
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

    public static final String TAG = Tokens.makeNamespace(FaunusCompiler.class) + ".tag";

    protected final Logger logger = Logger.getLogger(FaunusCompiler.class);
    private final String jobScript;

    private FaunusConfiguration configuration;
    private boolean derivationJob = true;

    private final List<Job> jobs = new ArrayList<Job>();
    private final List<Path> intermediateFiles = new ArrayList<Path>();

    private final Configuration mapSequenceConfiguration = new Configuration();
    private final List<Class<? extends Mapper>> mapSequenceClasses = new ArrayList<Class<? extends Mapper>>();
    private Class mapRClass = null;
    private Class reduceClass = null;


    private static final Class<? extends InputFormat> INTERMEDIATE_INPUT_FORMAT = SequenceFileInputFormat.class;
    private static final Class<? extends OutputFormat> INTERMEDIATE_OUTPUT_FORMAT = SequenceFileOutputFormat.class;


    public FaunusCompiler(final String jobScript, final Configuration conf) {
        this.jobScript = jobScript;
        this.configuration = new FaunusConfiguration(conf);
    }

    private String toStringOfJob(final Class sequenceClass) {
        final List<String> list = new ArrayList<String>();
        for (final Class klass : this.mapSequenceClasses) {
            list.add(klass.getCanonicalName());
        }
        if (null != mapRClass && null != reduceClass) {
            list.add(this.mapRClass.getCanonicalName());
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

    ///////////// TRANSFORMS

    public void transform(final Class<? extends Element> klass, final String function) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(TransformMap.CLOSURE, function);
        conf.setClass(TransformMap.CLASS, klass, Element.class);
        final Job job = new Job(conf, TransformMap.class.getCanonicalName());
        job.setMapperClass(TransformMap.Map.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        this.configureStatisticsJob(job);
        this.jobs.add(job);
    }

    public void _() {
        this.mapSequenceClasses.add(IdentityMap.Map.class);
    }

    public void vertexMap(final long... ids) {
        final String[] idStrings = new String[ids.length];
        for (int i = 0; i < ids.length; i++) {
            idStrings[i] = String.valueOf(ids[i]);
        }
        this.mapSequenceConfiguration.setStrings(VertexMap.IDS + "-" + this.mapSequenceClasses.size(), idStrings);
        this.mapSequenceClasses.add(VertexMap.Map.class);
    }

    public void verticesMap() {
        this.mapSequenceClasses.add(VerticesMap.Map.class);
    }

    public void edgesMap() {
        this.mapSequenceClasses.add(EdgesMap.Map.class);
    }

    public void verticesVerticesMapReduce(final Direction direction, final String... labels) throws IOException {
        this.mapSequenceConfiguration.set(VerticesVerticesMapReduce.DIRECTION, direction.name());
        this.mapSequenceConfiguration.setStrings(VerticesVerticesMapReduce.LABELS, labels);
        this.mapRClass = VerticesVerticesMapReduce.Map.class;
        this.reduceClass = VerticesVerticesMapReduce.Reduce.class;
        this.completeSequence();
    }

    public void verticesEdgesMapReduce(final Direction direction, final String... labels) throws IOException {
        this.mapSequenceConfiguration.set(VerticesEdgesMapReduce.DIRECTION, direction.name());
        this.mapSequenceConfiguration.setStrings(VerticesEdgesMapReduce.LABELS, labels);
        this.mapRClass = VerticesEdgesMapReduce.Map.class;
        this.reduceClass = VerticesEdgesMapReduce.Reduce.class;
        this.completeSequence();
    }
    
    public void edgesVerticesMap(final Direction direction) throws IOException  {
        this.mapSequenceConfiguration.set(EdgesVerticesMap.DIRECTION + "-" + this.mapSequenceClasses.size(), direction.name());    
        this.mapSequenceClasses.add(EdgesVerticesMap.Map.class);
    }


    public void pathMap(final Class<? extends Element> klass) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.setClass(PathMap.CLASS, klass, Element.class);
        final Job job = new Job(conf, PathMap.class.getCanonicalName());
        job.setMapperClass(PathMap.Map.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        this.configureStatisticsJob(job);
        this.jobs.add(job);
    }

    ///////////// FILTERS

    public void filterMap(final Class<? extends Element> klass, final String closure) {
        if (!klass.equals(Vertex.class) && !klass.equals(Edge.class)) {
            throw new RuntimeException("Unsupported element class: " + klass.getName());
        }

        this.mapSequenceConfiguration.setClass(FilterMap.CLASS + "-" + this.mapSequenceClasses.size(), klass, Element.class);
        this.mapSequenceConfiguration.set(FilterMap.CLOSURE + "-" + this.mapSequenceClasses.size(), closure);
        this.mapSequenceClasses.add(FilterMap.Map.class);
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
    }

    public void backFilterMapReduce(final Class<? extends Element> klass, final String step) throws IOException {
        this.mapSequenceConfiguration.set(BackFilterMapReduce.STEP, step);
        this.mapSequenceConfiguration.setClass(BackFilterMapReduce.CLASS, klass, Element.class);
        this.mapRClass = BackFilterMapReduce.Map.class;
        this.reduceClass = BackFilterMapReduce.Reduce.class;
        this.completeSequence();
    }

    ///////////// SIDE-EFFECTS

    public void as(final Class<? extends Element> klass, final String tag, final int step) {
        this.configuration.setInt(TAG + "." + tag, step);
    }


    public void linkMapReduce(final String step, final Direction direction, final String label) throws IOException {
        this.mapSequenceConfiguration.set(LinkMapReduce.STEP, step);
        this.mapSequenceConfiguration.set(LinkMapReduce.DIRECTION, direction.name());
        this.mapSequenceConfiguration.set(LinkMapReduce.LABEL, label);
        this.mapRClass = LinkMapReduce.Map.class;
        this.reduceClass = LinkMapReduce.Reduce.class;
        this.completeSequence();
    }

    public void commitEdgesMap(final Tokens.Action action) {
        this.mapSequenceConfiguration.set(CommitEdgesMap.ACTION + "-" + mapSequenceClasses.size(), action.name());
        this.mapSequenceClasses.add(CommitEdgesMap.Map.class);
    }

    public void commitVerticesMapReduce(final Tokens.Action action) throws IOException {
        this.mapSequenceConfiguration.set(CommitVerticesMapReduce.ACTION, action.name());
        this.mapRClass = CommitVerticesMapReduce.Map.class;
        this.reduceClass = CommitVerticesMapReduce.Reduce.class;
        this.completeSequence();
    }

    public void valueDistribution(final Class<? extends Element> klass, final String property) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(ValueGroupCountMapReduce.CLASS, klass.getName());
        conf.set(ValueGroupCountMapReduce.PROPERTY, property);
        final Job job = new Job(conf, ValueGroupCountMapReduce.class.getCanonicalName());
        job.setMapperClass(ValueGroupCountMapReduce.Map.class);
        job.setReducerClass(ValueGroupCountMapReduce.Reduce.class);
        job.setCombinerClass(ValueGroupCountMapReduce.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        this.configureStatisticsJob(job);
        this.jobs.add(job);
    }

    public void groupCountMapReduce(final Class<? extends Element> klass, final String keyClosure, final String valueClosure) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(GroupCountMapReduce.CLASS, klass.getName());
        conf.set(GroupCountMapReduce.KEY_FUNCTION, keyClosure);
        conf.set(GroupCountMapReduce.VALUE_FUNCTION, valueClosure);
        final Job job = new Job(conf, GroupCountMapReduce.class.getCanonicalName());
        job.setMapperClass(GroupCountMapReduce.Map.class);
        job.setReducerClass(GroupCountMapReduce.Reduce.class);
        job.setCombinerClass(GroupCountMapReduce.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        this.configureStatisticsJob(job);
        this.jobs.add(job);
    }

    /////////////////////////////// UTILITIES


    public void completeSequence() throws IOException {
        if (this.mapRClass != null && this.reduceClass != null) {
            this.mapSequenceConfiguration.set(MapReduceSequence.MAPR_CLASS, this.mapRClass.getName());
            this.mapSequenceConfiguration.set(MapReduceSequence.REDUCE_CLASS, this.reduceClass.getName());
            if (this.mapSequenceClasses.size() > 0) {
                this.mapSequenceConfiguration.setStrings(MapReduceSequence.MAP_CLASSES, toStringMapSequenceClasses());
            }
            final Job job = new Job(this.mapSequenceConfiguration, this.toStringOfJob(MapReduceSequence.class));
            job.setMapperClass(MapReduceSequence.Map.class);
            job.setReducerClass(MapReduceSequence.Reduce.class);
            this.configureMapReduceJob(job);
            this.jobs.add(job);

        } else {
            if (this.mapSequenceClasses.size() > 0) {
                this.mapSequenceConfiguration.setStrings(MapSequence.MAP_CLASSES, toStringMapSequenceClasses());
                final Job job = new Job(this.mapSequenceConfiguration, this.toStringOfJob(MapSequence.class));
                job.setMapperClass(MapSequence.Map.class);
                this.configureMapJob(job);
                this.jobs.add(job);
            }
        }

        this.mapRClass = null;
        this.reduceClass = null;
        this.mapSequenceClasses.clear();
        this.mapSequenceConfiguration.clear();
    }

    private void configureMapJob(final Job job) {
        job.setJarByClass(FaunusPipeline.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(FaunusVertex.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FaunusVertex.class);
    }

    private void configureMapReduceJob(final Job job) {
        job.setJarByClass(FaunusPipeline.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Holder.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FaunusVertex.class);
    }

    private void configureStatisticsJob(final Job job) {
        job.setJarByClass(FaunusPipeline.class);
        this.derivationJob = false;
    }

    public void composeJobs() throws IOException {
        if (this.jobs.size() == 0) {
            return;
        }

        final FileSystem hdfs = FileSystem.get(this.configuration);
        try {
            final Job startJob = this.jobs.get(0);
            startJob.setInputFormatClass(this.configuration.getGraphInputFormat());
            // configure input location
            if (this.configuration.getGraphInputFormat().equals(GraphSONInputFormat.class) || this.configuration.getGraphInputFormat().equals(SequenceFileInputFormat.class)) {
                FileInputFormat.setInputPaths(startJob, this.configuration.getInputLocation());
            } else if (this.configuration.getGraphInputFormat().equals(RexsterInputFormat.class)) {
                /* do nothing */
            } else if (this.configuration.getGraphInputFormat().equals(TitanCassandraInputFormat.class)) {
                ConfigHelper.setInputColumnFamily(this.configuration, ConfigHelper.getInputKeyspace(this.configuration), "edgestore");

                SlicePredicate predicate = new SlicePredicate();
                SliceRange sliceRange = new SliceRange();
                sliceRange.setStart(new byte[0]);
                sliceRange.setFinish(new byte[0]);
                predicate.setSlice_range(sliceRange);

                ConfigHelper.setInputSlicePredicate(this.configuration, predicate);
            } else
                throw new IOException(this.configuration.getGraphInputFormat().getName() + " is not a supported input format");


            if (this.jobs.size() > 1) {
                final Path path = new Path(UUID.randomUUID().toString());
                FileOutputFormat.setOutputPath(startJob, path);
                this.intermediateFiles.add(path);
                startJob.setOutputFormatClass(INTERMEDIATE_OUTPUT_FORMAT);
            } else {
                FileOutputFormat.setOutputPath(startJob, this.configuration.getOutputLocation());
                startJob.setOutputFormatClass(this.derivationJob ? this.configuration.getGraphOutputFormat() : this.configuration.getStatisticsOutputFormat());


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
                endJob.setOutputFormatClass(this.derivationJob ? this.configuration.getGraphOutputFormat() : this.configuration.getStatisticsOutputFormat());
                FileInputFormat.setInputPaths(endJob, this.intermediateFiles.get(this.intermediateFiles.size() - 1));
                FileOutputFormat.setOutputPath(endJob, this.configuration.getOutputLocation());
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

        for (final Job job : this.jobs) {
            for (final Map.Entry<String, String> entry : this.configuration) {
                job.getConfiguration().set(entry.getKey(), entry.getValue());
            }
        }
    }

    public int run(String[] args) throws Exception {
        if (this.configuration.getOutputLocationOverwrite()) {
            final FileSystem hdfs = FileSystem.get(this.getConf());
            if (hdfs.exists(this.configuration.getOutputLocation())) {
                hdfs.delete(this.configuration.getOutputLocation(), true);
            }
        }
        logger.info("        ,");
        logger.info("    ,   |\\ ,__");
        logger.info("    |\\   \\/   `\\");
        logger.info("    \\ `-.:.     `\\");
        logger.info("     `-.__ `\\/\\/\\|");
        logger.info("        / `'/ () \\");
        logger.info("      .'   /\\     )  Faunus: A Library of Hadoop-Based Graph Tools");
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
            job.waitForCompletion(true);
            if (i > 0 && this.intermediateFiles.size() > 0) {
                final FileSystem hdfs = FileSystem.get(job.getConfiguration());
                final Path path = this.intermediateFiles.remove(0);
                if (hdfs.exists(path))
                    hdfs.delete(path, true);
            }
        }
        return 0;
    }


}
