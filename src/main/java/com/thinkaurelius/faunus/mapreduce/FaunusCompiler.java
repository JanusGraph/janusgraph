package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.hdfs.NoSideEffectFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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

    protected final List<Job> jobs = new ArrayList<Job>();

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
        this.setConf(new Configuration());
        this.addConfiguration(this.graph.getConf());
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

    private void addConfiguration(final Configuration configuration) {
        for (final Map.Entry<String, String> entry : configuration) {
            if (entry.getKey().equals(PATH_ENABLED) & Boolean.valueOf(entry.getValue()))
                this.pathEnabled = true;
            this.getConf().set(entry.getKey() + "-" + this.mapSequenceClasses.size(), entry.getValue());
            this.getConf().set(entry.getKey(), entry.getValue());
        }
    }

    public void addMapReduce(final Class<? extends Mapper> mapper,
                             final Class<? extends Reducer> combiner,
                             final Class<? extends Reducer> reducer,
                             final Class<? extends WritableComparator> comparator,
                             final Class<? extends WritableComparable> mapOutputKey,
                             final Class<? extends WritableComparable> mapOutputValue,
                             final Class<? extends WritableComparable> reduceOutputKey,
                             final Class<? extends WritableComparable> reduceOutputValue,
                             final Configuration configuration) {

        this.addConfiguration(configuration);
        this.mapSequenceClasses.add(mapper);
        this.combinerClass = combiner;
        this.reduceClass = reducer;
        this.comparatorClass = comparator;
        this.mapOutputKey = mapOutputKey;
        this.mapOutputValue = mapOutputValue;
        this.outputKey = reduceOutputKey;
        this.outputValue = reduceOutputValue;
        this.completeSequence();
    }

    public void addMapReduce(final Class<? extends Mapper> mapper,
                             final Class<? extends Reducer> combiner,
                             final Class<? extends Reducer> reducer,
                             final Class<? extends WritableComparable> mapOutputKey,
                             final Class<? extends WritableComparable> mapOutputValue,
                             final Class<? extends WritableComparable> reduceOutputKey,
                             final Class<? extends WritableComparable> reduceOutputValue,
                             final Configuration configuration) {

        this.addConfiguration(configuration);
        this.mapSequenceClasses.add(mapper);
        this.combinerClass = combiner;
        this.reduceClass = reducer;
        this.mapOutputKey = mapOutputKey;
        this.mapOutputValue = mapOutputValue;
        this.outputKey = reduceOutputKey;
        this.outputValue = reduceOutputValue;
        this.completeSequence();
    }

    public void addMap(final Class<? extends Mapper> mapper,
                       final Class<? extends WritableComparable> mapOutputKey,
                       final Class<? extends WritableComparable> mapOutputValue,
                       final Configuration configuration) {

        this.addConfiguration(configuration);
        this.mapSequenceClasses.add(mapper);
        this.mapOutputKey = mapOutputKey;
        this.mapOutputValue = mapOutputValue;
        this.outputKey = mapOutputKey;
        this.outputValue = mapOutputValue;

    }

    public void completeSequence() {
        if (this.mapSequenceClasses.size() > 0) {
            this.getConf().setStrings(MapSequence.MAP_CLASSES, toStringMapSequenceClasses());
            final Job job;
            try {
                job = new Job(this.getConf(), this.toStringOfJob(MapSequence.class));
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
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

            job.setMapOutputKeyClass(this.mapOutputKey);
            job.setMapOutputValueClass(this.mapOutputValue);
            if (null != this.comparatorClass)
                job.setSortComparatorClass(this.comparatorClass);
            // else
            //   job.setSortComparatorClass(NullWritable.Comparator.class);
            job.setOutputKeyClass(this.outputKey);
            job.setOutputValueClass(this.outputValue);


            this.jobs.add(job);

            this.setConf(new Configuration());
            this.addConfiguration(this.graph.getConf());
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

        String hadoopFileJar = null;
        if (new File("target/" + Tokens.FAUNUS_JOB_JAR).exists()) {
            logger.warn("Using developer reference to target/" + Tokens.FAUNUS_JOB_JAR);
            hadoopFileJar = "target/" + Tokens.FAUNUS_JOB_JAR;
        } else if (new File("../target/" + Tokens.FAUNUS_JOB_JAR).exists()) {
            logger.warn("Using developer reference to target/" + Tokens.FAUNUS_JOB_JAR);
            hadoopFileJar = "../target/" + Tokens.FAUNUS_JOB_JAR;
        } else if (new File("lib/" + Tokens.FAUNUS_JOB_JAR).exists()) {
            logger.warn("Using distribution reference to lib/" + Tokens.FAUNUS_JOB_JAR);
            hadoopFileJar = "lib/" + Tokens.FAUNUS_JOB_JAR;
        } else if (new File("../lib/" + Tokens.FAUNUS_JOB_JAR).exists()) {
            logger.warn("Using distribution reference to lib/" + Tokens.FAUNUS_JOB_JAR);
            hadoopFileJar = "../lib/" + Tokens.FAUNUS_JOB_JAR;
        } else {
            final String faunusHome = System.getenv(Tokens.FAUNUS_HOME);
            if (null == faunusHome || faunusHome.isEmpty())
                throw new IllegalStateException("FAUNUS_HOME must be set in order to locate the Faunus Hadoop job jar: " + Tokens.FAUNUS_JOB_JAR);
            if (new File(faunusHome + "/lib/" + Tokens.FAUNUS_JOB_JAR).exists()) {
                hadoopFileJar = faunusHome + "/lib/" + Tokens.FAUNUS_JOB_JAR;
            }
        }
        if (null == hadoopFileJar)
            throw new IllegalStateException("The Faunus Hadoop job jar could not be found: " + Tokens.FAUNUS_JOB_JAR);

        if (this.pathEnabled)
            logger.warn("Path calculations are enabled for this Faunus job (space and time expensive)");

        final FileSystem hdfs = FileSystem.get(this.graph.getConf());
        final String outputJobPrefix = this.graph.getOutputLocation().toString() + "/" + Tokens.JOB;
        hdfs.mkdirs(this.graph.getOutputLocation());

        //////// CHAINING JOBS TOGETHER

        for (int i = 0; i < this.jobs.size(); i++) {
            final Job job = this.jobs.get(i);
            job.getConfiguration().setBoolean(PATH_ENABLED, this.pathEnabled);
            job.getConfiguration().set("mapred.jar", hadoopFileJar);

            FileOutputFormat.setOutputPath(job, new Path(outputJobPrefix + "-" + i));

            // configure job inputs
            if (i == 0) {
                job.setInputFormatClass(this.graph.getGraphInputFormat());
                if (FileInputFormat.class.isAssignableFrom(this.graph.getGraphInputFormat())) {
                    FileInputFormat.setInputPaths(job, this.graph.getInputLocation());
                    FileInputFormat.setInputPathFilter(job, NoSideEffectFilter.class);
                }
            } else {
                job.setInputFormatClass(INTERMEDIATE_INPUT_FORMAT);
                FileInputFormat.setInputPaths(job, new Path(outputJobPrefix + "-" + (i - 1)));
                FileInputFormat.setInputPathFilter(job, NoSideEffectFilter.class);
            }

            // configure job outputs
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