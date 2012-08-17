package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.graphson.GraphSONInputFormat;
import com.thinkaurelius.faunus.formats.titan.TitanCassandraInputFormat;
import com.thinkaurelius.faunus.mapreduce.Function;
import com.thinkaurelius.faunus.mapreduce.MapReduceSequence;
import com.thinkaurelius.faunus.mapreduce.MapSequence;
import com.thinkaurelius.faunus.mapreduce.derivations.DirectionFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.EdgeFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.EdgePropertyFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.Identity;
import com.thinkaurelius.faunus.mapreduce.derivations.LabelFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.LoopFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.Properties;
import com.thinkaurelius.faunus.mapreduce.derivations.SideEffect;
import com.thinkaurelius.faunus.mapreduce.derivations.Transpose;
import com.thinkaurelius.faunus.mapreduce.derivations.Traverse;
import com.thinkaurelius.faunus.mapreduce.derivations.VertexFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.VertexPropertyFilter;
import com.thinkaurelius.faunus.mapreduce.statistics.AdjacentProperties;
import com.thinkaurelius.faunus.mapreduce.statistics.Degree;
import com.thinkaurelius.faunus.mapreduce.statistics.DegreeDistribution;
import com.thinkaurelius.faunus.mapreduce.statistics.Distribution;
import com.thinkaurelius.faunus.mapreduce.statistics.LabelDistribution;
import com.thinkaurelius.faunus.mapreduce.statistics.PropertyDistribution;
import com.thinkaurelius.faunus.mapreduce.statistics.PropertyValueDistribution;
import com.thinkaurelius.faunus.mapreduce.statistics.SortedDegree;
import com.thinkaurelius.faunus.mapreduce.statistics.Transform;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import groovy.lang.Closure;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusGraph extends Configured implements Tool {

    private final Logger logger = Logger.getLogger(FaunusGraph.class);

    private Configuration configuration;
    private final Class<? extends InputFormat> inputFormat;
    private Class<? extends OutputFormat> outputFormat;
    private final Class<? extends OutputFormat> statisticsOutputFormat;
    private final Path outputPath;
    private final String jobScript;

    private final List<Job> jobs = new ArrayList<Job>();
    private final List<Path> intermediateFiles = new ArrayList<Path>();

    private final Configuration mapSequenceConfiguration = new Configuration();
    private final List<Class<? extends Mapper>> mapSequenceClasses = new ArrayList<Class<? extends Mapper>>();
    private Class mapRClass = null;
    private Class reduceClass = null;

    private static final Class<? extends InputFormat> intermediateInputFormat = SequenceFileInputFormat.class;
    private static final Class<? extends OutputFormat> intermediateOutputFormat = SequenceFileOutputFormat.class;

    public FaunusGraph V = this;

    public FaunusGraph(final String jobScript, final Configuration conf) throws ClassNotFoundException {
        this.configuration = conf;
        this.inputFormat = (Class<? extends InputFormat>) Class.forName(this.configuration.get(Tokens.GRAPH_INPUT_FORMAT_CLASS));
        this.outputFormat = (Class<? extends OutputFormat>) Class.forName(this.configuration.get(Tokens.GRAPH_OUTPUT_FORMAT_CLASS));

        this.statisticsOutputFormat = (Class<? extends OutputFormat>) Class.forName(this.configuration.get(Tokens.STATISTIC_OUTPUT_FORMAT_CLASS));
        this.outputPath = new Path(this.configuration.get(Tokens.DATA_OUTPUT_LOCATION));
        this.jobScript = jobScript;
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

    public FaunusGraph V() {
        return this;
    }

    ////////////////////////////////////////////////////////////////
    ///////////////////////// DERIVATIONS /////////////////////////
    ///////////////////////////////////////////////////////////////

    public FaunusGraph _() throws IOException {
        this.mapSequenceClasses.add(Identity.Map.class);
        return this;
    }

    // FILTERS

    public FaunusGraph filter(final Class<? extends Element> klass, final String function) throws IOException {
        if (klass.equals(Vertex.class)) {
            this.mapSequenceConfiguration.set(VertexFilter.FUNCTION, function);
            this.mapRClass = VertexFilter.Map.class;
            this.reduceClass = VertexFilter.Reduce.class;
            this.completeSequence();
        } else if (klass.equals(Edge.class)) {
            this.mapSequenceConfiguration.set(EdgeFilter.FUNCTION + "-" + this.mapSequenceClasses.size(), function);
            this.mapSequenceClasses.add(EdgeFilter.Map.class);
        } else {
            throw new IOException("Unsupported element class: " + klass.getName());
        }
        return this;
    }

    public FaunusGraph propertyFilter(final Class<? extends Element> klass, final String key, final Query.Compare compare, final Object value) throws IOException {
        return this.propertyFilter(klass, key, compare, value, false);
    }

    public FaunusGraph propertyFilter(final Class<? extends Element> klass, final String key, final Query.Compare compare, final Object value, final Boolean nullIsWildcard) throws IOException {
        if (klass.equals(Vertex.class)) {
            this.mapSequenceConfiguration.set(VertexPropertyFilter.KEY, key);
            this.mapSequenceConfiguration.set(VertexPropertyFilter.COMPARE, compare.name());
            this.mapSequenceConfiguration.set(VertexPropertyFilter.VALUE, value.toString());
            this.mapSequenceConfiguration.setBoolean(VertexPropertyFilter.NULL_WILDCARD, nullIsWildcard);
            if (value instanceof String) {
                this.mapSequenceConfiguration.setClass(VertexPropertyFilter.VALUE_CLASS, String.class, String.class);
            } else if (value instanceof Boolean) {
                this.mapSequenceConfiguration.setClass(VertexPropertyFilter.VALUE_CLASS, Boolean.class, Boolean.class);
            } else if (value instanceof Number) {
                this.mapSequenceConfiguration.setClass(VertexPropertyFilter.VALUE_CLASS, Number.class, Number.class);
            }
            this.mapRClass = VertexPropertyFilter.Map.class;
            this.reduceClass = VertexPropertyFilter.Reduce.class;
            this.completeSequence();
        } else if (klass.equals(Edge.class)) {
            this.mapSequenceConfiguration.set(EdgePropertyFilter.KEY + "-" + this.mapSequenceClasses.size(), key);
            this.mapSequenceConfiguration.set(EdgePropertyFilter.COMPARE + "-" + this.mapSequenceClasses.size(), compare.name());
            this.mapSequenceConfiguration.set(EdgePropertyFilter.VALUE + "-" + this.mapSequenceClasses.size(), value.toString());
            this.mapSequenceConfiguration.setBoolean(EdgePropertyFilter.NULL_WILDCARD, nullIsWildcard);
            if (value instanceof String) {
                this.mapSequenceConfiguration.setClass(EdgePropertyFilter.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), String.class, String.class);
            } else if (value instanceof Boolean) {
                this.mapSequenceConfiguration.setClass(EdgePropertyFilter.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), Boolean.class, Boolean.class);
            } else if (value instanceof Number) {
                this.mapSequenceConfiguration.setClass(EdgePropertyFilter.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), Number.class, Number.class);
            }
            this.mapSequenceClasses.add(EdgePropertyFilter.Map.class);
        } else {
            throw new IOException("Unsupported element class: " + klass.getName());
        }
        return this;
    }

    public FaunusGraph directionFilter(final Direction direction) {
        this.mapSequenceConfiguration.set(DirectionFilter.DIRECTION + "-" + this.mapSequenceClasses.size(), direction.name());
        this.mapSequenceClasses.add(DirectionFilter.Map.class);
        return this;
    }

    public FaunusGraph labelFilter(final Tokens.Action action, final String... labels) {
        this.mapSequenceConfiguration.set(LabelFilter.ACTION + "-" + this.mapSequenceClasses.size(), action.name());
        this.mapSequenceConfiguration.setStrings(LabelFilter.LABELS + "-" + this.mapSequenceClasses.size(), labels);
        this.mapSequenceClasses.add(LabelFilter.Map.class);
        return this;
    }

    public FaunusGraph loopFilter(final Tokens.Action action, final String... labels) {
        this.mapSequenceConfiguration.set(LoopFilter.ACTION + "-" + this.mapSequenceClasses.size(), action.name());
        this.mapSequenceConfiguration.setStrings(LoopFilter.LABELS + "-" + this.mapSequenceClasses.size(), labels);
        this.mapSequenceClasses.add(LoopFilter.Map.class);
        return this;
    }

    // SIDEEFFECT

    public FaunusGraph sideEffect(final Class<? extends Element> klass, final String function) {
        this.mapSequenceConfiguration.set(SideEffect.CLASS + "-" + this.mapSequenceClasses.size(), klass.getName());
        this.mapSequenceConfiguration.set(SideEffect.FUNCTION + "-" + this.mapSequenceClasses.size(), function);
        this.mapSequenceClasses.add(SideEffect.Map.class);
        return this;
    }

    public FaunusGraph properties(final Class<? extends Element> klass, final Tokens.Action action, final String... keys) {
        this.mapSequenceConfiguration.set(Properties.ACTION + "-" + this.mapSequenceClasses.size(), action.name());
        this.mapSequenceConfiguration.set(Properties.CLASS + "-" + this.mapSequenceClasses.size(), klass.getName());
        this.mapSequenceConfiguration.setStrings(Properties.KEYS + "-" + this.mapSequenceClasses.size(), keys);
        this.mapSequenceClasses.add(Properties.Map.class);
        return this;
    }

    public FaunusGraph transpose(final String label, final String newLabel, final Tokens.Action action) {
        this.mapSequenceConfiguration.set(Transpose.LABEL + "-" + this.mapSequenceClasses.size(), label);
        this.mapSequenceConfiguration.set(Transpose.NEW_LABEL + "-" + this.mapSequenceClasses.size(), newLabel);
        this.mapSequenceConfiguration.set(Transpose.ACTION + "-" + this.mapSequenceClasses.size(), action.name());
        this.mapSequenceClasses.add(Transpose.Map.class);
        return this;
    }

    public FaunusGraph traverse(final Direction firstDirection, final String firstLabel, final Direction secondDirection, final String secondLabel, final String newLabel, final Tokens.Action action) throws IOException {
        this.mapSequenceConfiguration.set(Traverse.FIRST_DIRECTION, firstDirection.toString());
        this.mapSequenceConfiguration.set(Traverse.FIRST_LABEL, firstLabel);
        this.mapSequenceConfiguration.set(Traverse.SECOND_DIRECTION, secondDirection.toString());
        this.mapSequenceConfiguration.set(Traverse.SECOND_LABEL, secondLabel);
        this.mapSequenceConfiguration.set(Traverse.NEW_LABEL, newLabel);
        this.mapSequenceConfiguration.set(Traverse.ACTION, action.name());
        this.mapRClass = Traverse.Map.class;
        this.reduceClass = Traverse.Reduce.class;
        this.completeSequence();
        return this;
    }

    private FaunusGraph completeSequence() throws IOException {
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
        return this;
    }

    private void configureMapJob(final Job job) {
        job.setJarByClass(FaunusGraph.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(FaunusVertex.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FaunusVertex.class);
    }

    private void configureMapReduceJob(final Job job) {
        job.setJarByClass(FaunusGraph.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Holder.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FaunusVertex.class);
    }

    ///////////////////////////////////////////////////////////////
    ///////////////////////// STATISTICS /////////////////////////
    //////////////////////////////////////////////////////////////

    public FaunusGraph adjacentProperties(final String property, final String... labels) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(AdjacentProperties.PROPERTY, property);
        conf.setStrings(AdjacentProperties.LABELS, labels);
        final Job job1 = new Job(conf, AdjacentProperties.class.getCanonicalName() + ":part-1");
        job1.setMapperClass(AdjacentProperties.Map.class);
        job1.setReducerClass(AdjacentProperties.Reduce.class);
        job1.setJarByClass(FaunusGraph.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Holder.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        this.jobs.add(job1);

        // todo: aggregate or not?

        final Job job2 = new Job(new Configuration(), AdjacentProperties.class.getCanonicalName() + ":part-2");
        job2.setMapperClass(AdjacentProperties.Map2.class);
        job2.setCombinerClass(AdjacentProperties.Reduce2.class);
        job2.setReducerClass(AdjacentProperties.Reduce2.class);
        job2.setJarByClass(FaunusGraph.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        this.jobs.add(job2);

        this.outputFormat = this.statisticsOutputFormat;
        return this;


    }

    public FaunusGraph transform(final String function) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(Transform.FUNCTION, function);
        final Job job = new Job(conf, Transform.class.getCanonicalName());
        job.setMapperClass(Transform.Map.class);
        job.setJarByClass(FaunusGraph.class);
        job.setMapOutputKeyClass(FaunusVertex.class);
        job.setMapOutputValueClass(Transform.class);
        this.outputFormat = this.statisticsOutputFormat;
        this.jobs.add(job);
        return this;
    }

    public FaunusGraph degree(final String property, final Direction direction, final String... labels) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(Degree.PROPERTY, property);
        conf.set(Degree.DIRECTION, direction.name());
        conf.setStrings(Degree.LABELS, labels);
        final Job job = new Job(conf, Degree.class.getCanonicalName());
        job.setMapperClass(Degree.Map.class);
        job.setJarByClass(FaunusGraph.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        this.outputFormat = this.statisticsOutputFormat;
        this.jobs.add(job);
        return this;
    }

    public FaunusGraph degree(final String property, final Tokens.Order order, final Direction direction, final String... labels) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(SortedDegree.PROPERTY, property);
        conf.set(SortedDegree.DIRECTION, direction.name());
        conf.setStrings(SortedDegree.LABELS, labels);
        conf.set(SortedDegree.ORDER, order.name());
        final Job job = new Job(conf, SortedDegree.class.getCanonicalName());
        job.setMapperClass(SortedDegree.Map.class);
        job.setReducerClass(SortedDegree.Reduce.class);
        job.setJarByClass(FaunusGraph.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FaunusVertex.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        this.outputFormat = this.statisticsOutputFormat;
        this.jobs.add(job);
        return this;
    }

    public FaunusGraph distribution(final Class<? extends Element> klass, final String function) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(Distribution.CLASS, klass.getName());
        conf.set(Distribution.FUNCTION, function);
        final Job job = new Job(conf, Distribution.class.getCanonicalName());
        job.setMapperClass(Distribution.Map.class);
        job.setReducerClass(Distribution.Reduce.class);
        job.setCombinerClass(Distribution.Reduce.class);
        job.setJarByClass(FaunusGraph.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        this.outputFormat = this.statisticsOutputFormat;
        this.jobs.add(job);
        return this;
    }

    public FaunusGraph degreeDistribution(final Direction direction, final String... labels) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(DegreeDistribution.DIRECTION, direction.name());
        conf.setStrings(DegreeDistribution.LABELS, labels);
        final Job job = new Job(conf, DegreeDistribution.class.getCanonicalName());
        job.setMapperClass(DegreeDistribution.Map.class);
        job.setReducerClass(DegreeDistribution.Reduce.class);
        job.setCombinerClass(DegreeDistribution.Reduce.class);
        job.setJarByClass(FaunusGraph.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        this.outputFormat = this.statisticsOutputFormat;
        this.jobs.add(job);
        return this;
    }

    public FaunusGraph labelDistribution(final Direction direction) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(LabelDistribution.DIRECTION, direction.name());
        final Job job = new Job(conf, LabelDistribution.class.getCanonicalName());
        job.setMapperClass(LabelDistribution.Map.class);
        job.setReducerClass(LabelDistribution.Reduce.class);
        job.setCombinerClass(LabelDistribution.Reduce.class);
        job.setJarByClass(FaunusGraph.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        this.outputFormat = this.statisticsOutputFormat;
        this.jobs.add(job);
        return this;
    }

    public FaunusGraph propertyValueDistribution(final Class<? extends Element> klass, final String property) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(PropertyValueDistribution.CLASS, klass.getName());
        conf.set(PropertyValueDistribution.PROPERTY, property);
        final Job job = new Job(conf, PropertyValueDistribution.class.getCanonicalName());
        job.setMapperClass(PropertyValueDistribution.Map.class);
        job.setReducerClass(PropertyValueDistribution.Reduce.class);
        job.setCombinerClass(PropertyValueDistribution.Reduce.class);
        job.setJarByClass(FaunusGraph.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        this.outputFormat = this.statisticsOutputFormat;
        this.jobs.add(job);
        return this;
    }

    public FaunusGraph propertyDistribution(final Class<? extends Element> klass) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(PropertyDistribution.CLASS, klass.getName());
        final Job job = new Job(conf, PropertyDistribution.class.getCanonicalName());
        job.setMapperClass(PropertyDistribution.Map.class);
        job.setReducerClass(PropertyDistribution.Reduce.class);
        job.setCombinerClass(PropertyDistribution.Reduce.class);
        job.setJarByClass(FaunusGraph.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        this.outputFormat = this.statisticsOutputFormat;
        this.jobs.add(job);
        return this;
    }

    public int run(String[] args) throws Exception {
        if (this.configuration.getBoolean(Tokens.OVERWRITE_DATA_OUTPUT_LOCATION, false)) {
            final FileSystem hdfs = FileSystem.get(this.getConf());
            if (hdfs.exists(this.outputPath)) {
                hdfs.delete(this.outputPath, true);
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
        //logger.info("Faunus configuration: " + configurationToMap(this.configuration));
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

    private void composeJobs() throws IOException {
        if (this.jobs.size() == 0) {
            return;
        }

        final FileSystem hdfs = FileSystem.get(this.getConf());
        try {
            final Job startJob = this.jobs.get(0);
            startJob.setInputFormatClass(this.inputFormat);
            // configure input location
            if (this.inputFormat.equals(GraphSONInputFormat.class) || this.inputFormat.equals(SequenceFileInputFormat.class)) {
                FileInputFormat.setInputPaths(startJob, new Path(this.configuration.get(Tokens.GRAPH_INPUT_LOCATION)));
            } else if (this.inputFormat.equals(TitanCassandraInputFormat.class)) {
                ConfigHelper.setInputColumnFamily(this.configuration, ConfigHelper.getInputKeyspace(this.configuration), "User");
                SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes("age"), ByteBufferUtil.bytes("first"), ByteBufferUtil.bytes("last")));
                ConfigHelper.setInputSlicePredicate(this.configuration, predicate);
            } else
                throw new IOException(this.inputFormat.getName() + " is not a supported input format");


            if (this.jobs.size() > 1) {
                final Path path = new Path(UUID.randomUUID().toString());
                FileOutputFormat.setOutputPath(startJob, path);
                this.intermediateFiles.add(path);
                startJob.setOutputFormatClass(intermediateOutputFormat);
            } else {
                FileOutputFormat.setOutputPath(startJob, this.outputPath);
                startJob.setOutputFormatClass(this.outputFormat);

            }

            if (this.jobs.size() > 2) {
                for (int i = 1; i < this.jobs.size() - 1; i++) {
                    final Job midJob = this.jobs.get(i);
                    midJob.setInputFormatClass(intermediateInputFormat);
                    midJob.setOutputFormatClass(intermediateOutputFormat);
                    FileInputFormat.setInputPaths(midJob, this.intermediateFiles.get(this.intermediateFiles.size() - 1));
                    final Path path = new Path(UUID.randomUUID().toString());
                    FileOutputFormat.setOutputPath(midJob, path);
                    this.intermediateFiles.add(path);
                }
            }
            if (this.jobs.size() > 1) {
                final Job endJob = this.jobs.get(this.jobs.size() - 1);
                endJob.setInputFormatClass(intermediateInputFormat);
                endJob.setOutputFormatClass(this.outputFormat);
                FileInputFormat.setInputPaths(endJob, this.intermediateFiles.get(this.intermediateFiles.size() - 1));
                FileOutputFormat.setOutputPath(endJob, this.outputPath);
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

    public static void main(String[] args) throws Exception {
        if (args.length != 1 && args.length != 2) {
            System.out.println("Faunus: A Library of Graph-Based Hadoop Tools");
            System.out.println("Usage:");
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

        final FaunusGraph faunusGraph = new FaunusGraph(script, conf);
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
        scriptEngine.eval("EQUAL=" + Query.Compare.class.getName() + ".EQUAL");
        scriptEngine.eval("NOT_EQUAL=" + Query.Compare.class.getName() + ".NOT_EQUAL");
        scriptEngine.eval("LESS_THAN=" + Query.Compare.class.getName() + ".LESS_THAN");
        scriptEngine.eval("LESS_THAN_EQUAL=" + Query.Compare.class.getName() + ".LESS_THAN_EQUAL");
        scriptEngine.eval("GREATER_THAN=" + Query.Compare.class.getName() + ".GREATER_THAN");
        scriptEngine.eval("GREATER_THAN_EQUAL=" + Query.Compare.class.getName() + ".GREATER_THAN_EQUAL");


        scriptEngine.put("g", faunusGraph);
        ((FaunusGraph) scriptEngine.eval(script)).completeSequence();
        int result = ToolRunner.run(faunusGraph, args);
        System.exit(result);
    }

    public class GroovyFunction<A, B> implements Function<A, B> {
        private final Closure closure;

        public GroovyFunction(Closure closure) {
            this.closure = closure;
        }

        public B compute(A a) {
            return (B) this.closure.call(a);
        }
    }

}
