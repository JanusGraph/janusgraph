package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusConfiguration;
import com.thinkaurelius.faunus.FaunusPipeline;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.formats.graphson.GraphSONInputFormat;
import com.thinkaurelius.faunus.formats.titan.TitanCassandraInputFormat;
import com.thinkaurelius.faunus.mapreduce.derivations.CloseLine;
import com.thinkaurelius.faunus.mapreduce.derivations.CloseTriangle;
import com.thinkaurelius.faunus.mapreduce.derivations.DirectionFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.EdgeFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.EdgeValueFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.Identity;
import com.thinkaurelius.faunus.mapreduce.derivations.KeyFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.LabelFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.LoopFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.SideEffect;
import com.thinkaurelius.faunus.mapreduce.derivations.VertexFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.VertexValueFilter;
import com.thinkaurelius.faunus.mapreduce.statistics.AdjacentProperties;
import com.thinkaurelius.faunus.mapreduce.statistics.Degree;
import com.thinkaurelius.faunus.mapreduce.statistics.DegreeDistribution;
import com.thinkaurelius.faunus.mapreduce.statistics.GroupCount;
import com.thinkaurelius.faunus.mapreduce.statistics.KeyDistribution;
import com.thinkaurelius.faunus.mapreduce.statistics.Property;
import com.thinkaurelius.faunus.mapreduce.statistics.SortedDegree;
import com.thinkaurelius.faunus.mapreduce.statistics.Transform;
import com.thinkaurelius.faunus.mapreduce.statistics.ValueDistribution;
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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusRunner extends Configured implements Tool {

    protected final Logger logger = Logger.getLogger(FaunusRunner.class);
    private final String jobScript;

    private FaunusConfiguration configuration;
    protected boolean derivationJob = true;

    public final List<Job> jobs = new ArrayList<Job>();
    public final List<Path> intermediateFiles = new ArrayList<Path>();

    private final Configuration mapSequenceConfiguration = new Configuration();
    private final List<Class<? extends Mapper>> mapSequenceClasses = new ArrayList<Class<? extends Mapper>>();
    private Class mapRClass = null;
    private Class reduceClass = null;


    protected static final Class<? extends InputFormat> INTERMEDIATE_INPUT_FORMAT = SequenceFileInputFormat.class;
    protected static final Class<? extends OutputFormat> INTERMEDIATE_OUTPUT_FORMAT = SequenceFileOutputFormat.class;


    public FaunusRunner(final String jobScript, final Configuration conf) {
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


    ////////////////////////////////////////////////////////////////
    ///////////////////////// DERIVATIONS /////////////////////////
    ///////////////////////////////////////////////////////////////

    public void _() throws IOException {
        this.mapSequenceClasses.add(Identity.Map.class);
    }

    // FILTERS

    public void filter(final Class<? extends Element> klass, final String function) throws IOException {
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
    }

    public void propertyFilter(final Class<? extends Element> klass, final String key, final Query.Compare compare, final Object... values) throws IOException {
        this.propertyFilter(klass, false, key, compare, values);
    }

    public void propertyFilter(final Class<? extends Element> klass, final Boolean nullIsWildcard, final String key, final Query.Compare compare, final Object... values) throws IOException {
        String[] valueStrings = new String[values.length];
        for(int i=0; i<values.length; i++) {
            valueStrings[i] = values[i].toString();
        }
        
        if (klass.equals(Vertex.class)) {
            this.mapSequenceConfiguration.set(VertexValueFilter.KEY, key);
            this.mapSequenceConfiguration.set(VertexValueFilter.COMPARE, compare.name());
            this.mapSequenceConfiguration.setStrings(VertexValueFilter.VALUES, valueStrings);
            this.mapSequenceConfiguration.setBoolean(VertexValueFilter.NULL_WILDCARD, nullIsWildcard);
            if (values[0] instanceof String) {
                this.mapSequenceConfiguration.setClass(VertexValueFilter.VALUE_CLASS, String.class, String.class);
            } else if (values[0] instanceof Boolean) {
                this.mapSequenceConfiguration.setClass(VertexValueFilter.VALUE_CLASS, Boolean.class, Boolean.class);
            } else if (values[0] instanceof Number) {
                this.mapSequenceConfiguration.setClass(VertexValueFilter.VALUE_CLASS, Number.class, Number.class);
            }
            this.mapRClass = VertexValueFilter.Map.class;
            this.reduceClass = VertexValueFilter.Reduce.class;
            this.completeSequence();
        } else if (klass.equals(Edge.class)) {
            this.mapSequenceConfiguration.set(EdgeValueFilter.KEY + "-" + this.mapSequenceClasses.size(), key);
            this.mapSequenceConfiguration.set(EdgeValueFilter.COMPARE + "-" + this.mapSequenceClasses.size(), compare.name());
            this.mapSequenceConfiguration.setStrings(EdgeValueFilter.VALUES + "-" + this.mapSequenceClasses.size(), valueStrings);
            this.mapSequenceConfiguration.setBoolean(EdgeValueFilter.NULL_WILDCARD, nullIsWildcard);
            if (values[0] instanceof String) {
                this.mapSequenceConfiguration.setClass(EdgeValueFilter.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), String.class, String.class);
            } else if (values[0] instanceof Boolean) {
                this.mapSequenceConfiguration.setClass(EdgeValueFilter.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), Boolean.class, Boolean.class);
            } else if (values[0] instanceof Number) {
                this.mapSequenceConfiguration.setClass(EdgeValueFilter.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), Number.class, Number.class);
            }
            this.mapSequenceClasses.add(EdgeValueFilter.Map.class);
        } else {
            throw new IOException("Unsupported element class: " + klass.getName());
        }
    }

    public void directionFilter(final Direction direction) {
        this.mapSequenceConfiguration.set(DirectionFilter.DIRECTION + "-" + this.mapSequenceClasses.size(), direction.name());
        this.mapSequenceClasses.add(DirectionFilter.Map.class);
    }

    public void labelFilter(final Tokens.Action action, final String... labels) {
        this.mapSequenceConfiguration.set(LabelFilter.ACTION + "-" + this.mapSequenceClasses.size(), action.name());
        this.mapSequenceConfiguration.setStrings(LabelFilter.LABELS + "-" + this.mapSequenceClasses.size(), labels);
        this.mapSequenceClasses.add(LabelFilter.Map.class);
    }

    public void loopFilter(final Tokens.Action action, final String... labels) {
        this.mapSequenceConfiguration.set(LoopFilter.ACTION + "-" + this.mapSequenceClasses.size(), action.name());
        this.mapSequenceConfiguration.setStrings(LoopFilter.LABELS + "-" + this.mapSequenceClasses.size(), labels);
        this.mapSequenceClasses.add(LoopFilter.Map.class);
    }

    // SIDEEFFECT

    public void sideEffect(final Class<? extends Element> klass, final String function) {
        this.mapSequenceConfiguration.set(SideEffect.CLASS + "-" + this.mapSequenceClasses.size(), klass.getName());
        this.mapSequenceConfiguration.set(SideEffect.FUNCTION + "-" + this.mapSequenceClasses.size(), function);
        this.mapSequenceClasses.add(SideEffect.Map.class);
    }

    public void properties(final Class<? extends Element> klass, final Tokens.Action action, final String... keys) {
        this.mapSequenceConfiguration.set(KeyFilter.ACTION + "-" + this.mapSequenceClasses.size(), action.name());
        this.mapSequenceConfiguration.set(KeyFilter.CLASS + "-" + this.mapSequenceClasses.size(), klass.getName());
        this.mapSequenceConfiguration.setStrings(KeyFilter.KEYS + "-" + this.mapSequenceClasses.size(), keys);
        this.mapSequenceClasses.add(KeyFilter.Map.class);
    }

    public void transpose(final String label, final String newLabel, final Tokens.Action action, final boolean opposite) {
        this.mapSequenceConfiguration.set(CloseLine.LABEL + "-" + this.mapSequenceClasses.size(), label);
        this.mapSequenceConfiguration.set(CloseLine.NEW_LABEL + "-" + this.mapSequenceClasses.size(), newLabel);
        this.mapSequenceConfiguration.set(CloseLine.ACTION + "-" + this.mapSequenceClasses.size(), action.name());
        this.mapSequenceConfiguration.setBoolean(CloseLine.OPPOSITE + "-" + this.mapSequenceClasses.size(), opposite);
        this.mapSequenceClasses.add(CloseLine.Map.class);
    }

    public void traverse(final Direction firstDirection, final String firstLabel, final Direction secondDirection, final String secondLabel, final String newLabel, final Tokens.Action action) throws IOException {
        this.mapSequenceConfiguration.set(CloseTriangle.FIRST_DIRECTION, firstDirection.toString());
        this.mapSequenceConfiguration.setStrings(CloseTriangle.FIRST_LABELS, firstLabel);
        this.mapSequenceConfiguration.set(CloseTriangle.SECOND_DIRECTION, secondDirection.toString());
        this.mapSequenceConfiguration.setStrings(CloseTriangle.SECOND_LABELS, secondLabel);
        this.mapSequenceConfiguration.set(CloseTriangle.NEW_LABEL, newLabel);
        this.mapSequenceConfiguration.set(CloseTriangle.ACTION, action.name());
        this.mapRClass = CloseTriangle.Map.class;
        this.reduceClass = CloseTriangle.Reduce.class;
        this.completeSequence();
    }

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

    ///////////////////////////////////////////////////////////////
    ///////////////////////// STATISTICS /////////////////////////
    //////////////////////////////////////////////////////////////

    public void configureStatisticsJob(final Job job) {
        job.setJarByClass(FaunusPipeline.class);
        this.derivationJob = false;
    }

    public void transform(final String function) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(Transform.FUNCTION, function);
        final Job job = new Job(conf, Transform.class.getCanonicalName());
        job.setMapperClass(Transform.Map.class);
        job.setMapOutputKeyClass(FaunusVertex.class);
        job.setMapOutputValueClass(Text.class);
        this.configureStatisticsJob(job);
        this.jobs.add(job);
    }

    public void distribution(final Class<? extends Element> klass, final String keyFunction, final String valueFunction) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(GroupCount.CLASS, klass.getName());
        conf.set(GroupCount.KEY_FUNCTION, keyFunction);
        conf.set(GroupCount.VALUE_FUNCTION, valueFunction);
        final Job job = new Job(conf, GroupCount.class.getCanonicalName());
        job.setMapperClass(GroupCount.Map.class);
        job.setReducerClass(GroupCount.Reduce.class);
        job.setCombinerClass(GroupCount.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        this.configureStatisticsJob(job);
        this.jobs.add(job);
    }

    public void distribution(final Class<? extends Element> klass, final String keyFunction) throws IOException {
        this.distribution(klass, keyFunction, "{it -> 1l}");
    }

    public void adjacentProperties(final String property, final String... labels) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(AdjacentProperties.PROPERTY, property);
        conf.setStrings(AdjacentProperties.LABELS, labels);
        final Job job1 = new Job(conf, AdjacentProperties.class.getCanonicalName() + ":part-1");
        job1.setMapperClass(AdjacentProperties.Map.class);
        job1.setReducerClass(AdjacentProperties.Reduce.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Holder.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        this.configureStatisticsJob(job1);
        this.jobs.add(job1);

        // todo: aggregate or not?

        final Job job2 = new Job(new Configuration(), AdjacentProperties.class.getCanonicalName() + ":part-2");
        job2.setMapperClass(AdjacentProperties.Map2.class);
        job2.setCombinerClass(AdjacentProperties.Reduce2.class);
        job2.setReducerClass(AdjacentProperties.Reduce2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        this.configureStatisticsJob(job2);
        this.jobs.add(job2);
    }

    public void degree(final String property, final Direction direction, final String... labels) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(Degree.PROPERTY, property);
        conf.set(Degree.DIRECTION, direction.name());
        conf.setStrings(Degree.LABELS, labels);
        final Job job = new Job(conf, Degree.class.getCanonicalName());
        job.setMapperClass(Degree.Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        this.configureStatisticsJob(job);
        this.jobs.add(job);
    }

    public void degree(final String property, final Tokens.Order order, final Direction direction, final String... labels) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(SortedDegree.PROPERTY, property);
        conf.set(SortedDegree.DIRECTION, direction.name());
        conf.setStrings(SortedDegree.LABELS, labels);
        conf.set(SortedDegree.ORDER, order.name());
        final Job job = new Job(conf, SortedDegree.class.getCanonicalName());
        job.setMapperClass(SortedDegree.Map.class);
        job.setReducerClass(SortedDegree.Reduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FaunusVertex.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        this.configureStatisticsJob(job);
        this.jobs.add(job);
    }

    public void degreeDistribution(final Direction direction, final String... labels) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(DegreeDistribution.DIRECTION, direction.name());
        conf.setStrings(DegreeDistribution.LABELS, labels);
        final Job job = new Job(conf, DegreeDistribution.class.getCanonicalName());
        job.setMapperClass(DegreeDistribution.Map.class);
        job.setReducerClass(DegreeDistribution.Reduce.class);
        job.setCombinerClass(DegreeDistribution.Reduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        this.configureStatisticsJob(job);
        this.jobs.add(job);
    }


    public void valueDistribution(final Class<? extends Element> klass, final String property) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(ValueDistribution.CLASS, klass.getName());
        conf.set(ValueDistribution.PROPERTY, property);
        final Job job = new Job(conf, ValueDistribution.class.getCanonicalName());
        job.setMapperClass(ValueDistribution.Map.class);
        job.setReducerClass(ValueDistribution.Reduce.class);
        job.setCombinerClass(ValueDistribution.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        this.configureStatisticsJob(job);
        this.jobs.add(job);
    }

    public void keyDistribution(final Class<? extends Element> klass) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(KeyDistribution.CLASS, klass.getName());
        final Job job = new Job(conf, KeyDistribution.class.getCanonicalName());
        job.setMapperClass(KeyDistribution.Map.class);
        job.setReducerClass(KeyDistribution.Reduce.class);
        job.setCombinerClass(KeyDistribution.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        this.configureStatisticsJob(job);
        this.jobs.add(job);
    }

    public void label(final Class<? extends Element> klass) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(Property.PROPERTY, Tokens.LABEL);
        conf.set(Property.CLASS, klass.getName());
        final Job job = new Job(conf, Property.class.getCanonicalName());
        job.setMapperClass(Property.Map.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        this.configureStatisticsJob(job);
        this.jobs.add(job);
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
