package com.thinkaurelius.titan.hadoop.compat;

import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.config.ConfigurationUtil;
import com.thinkaurelius.titan.hadoop.config.HybridConfigured;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.FormatTools;
import com.thinkaurelius.titan.hadoop.formats.JobConfigurationFormat;
import com.thinkaurelius.titan.hadoop.hdfs.NoSideEffectFilter;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Hadoop1Compiler extends HybridConfigured implements HadoopCompiler {

    private static final String MAPRED_COMPRESS_MAP_OUTPUT = "mapred.compress.map.output";
    private static final String MAPRED_MAP_OUTPUT_COMPRESSION_CODEC = "mapred.map.output.compression.codec";
    private static final String MAPRED_JAR = "mapred.jar";

    public static final Logger logger = Logger.getLogger(Hadoop1Compiler.class);

    private HadoopGraph graph;

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

    public Hadoop1Compiler(final HadoopGraph graph) {
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
            this.getConf().set(entry.getKey() + "-" + this.mapSequenceClasses.size(), entry.getValue()); // TODO why does this line exist?
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

            job.setJarByClass(HadoopCompiler.class);
            job.setMapperClass(MapSequence.Map.class);
            if (null != this.reduceClass) {
                job.setReducerClass(this.reduceClass);
                if (null != this.combinerClass)
                    job.setCombinerClass(this.combinerClass);
                // if there is a reduce task, compress the map output to limit network traffic
                if (null == job.getConfiguration().get(MAPRED_COMPRESS_MAP_OUTPUT, null))
                    job.getConfiguration().setBoolean(MAPRED_COMPRESS_MAP_OUTPUT, true);
                if (null == job.getConfiguration().get(MAPRED_MAP_OUTPUT_COMPRESSION_CODEC, null))
                    job.getConfiguration().setClass(MAPRED_MAP_OUTPUT_COMPRESSION_CODEC, DefaultCodec.class, CompressionCodec.class);
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

        String hadoopFileJar = graph.getConf().get(MAPRED_JAR, null);
        if (null == hadoopFileJar) {
            if (new File("target/" + Tokens.TITAN_HADOOP_JOB_JAR).exists()) {
                hadoopFileJar = "target/" + Tokens.TITAN_HADOOP_JOB_JAR;
                logger.warn("Using the developer Titan/Hadoop job jar: " + hadoopFileJar);
            } else if (new File("../target/" + Tokens.TITAN_HADOOP_JOB_JAR).exists()) {
                hadoopFileJar = "../target/" + Tokens.TITAN_HADOOP_JOB_JAR;
                logger.warn("Using the developer Titan/Hadoop job jar: " + hadoopFileJar);
            } else if (new File("lib/" + Tokens.TITAN_HADOOP_JOB_JAR).exists()) {
                hadoopFileJar = "lib/" + Tokens.TITAN_HADOOP_JOB_JAR;
                logger.warn("Using the distribution Titan/Hadoop job jar: " + hadoopFileJar);
            } else if (new File("../lib/" + Tokens.TITAN_HADOOP_JOB_JAR).exists()) {
                hadoopFileJar = "../lib/" + Tokens.TITAN_HADOOP_JOB_JAR;
                logger.warn("Using the distribution Titan/Hadoop job jar: " + hadoopFileJar);
            } else {
                final String titanHadoopHome = System.getenv(Tokens.TITAN_HADOOP_HOME);
                if (null == titanHadoopHome || titanHadoopHome.isEmpty())
                    throw new IllegalStateException("TITAN_HADOOP_HOME must be set in order to locate the Titan/Hadoop job jar: " + Tokens.TITAN_HADOOP_JOB_JAR);
                if (new File(titanHadoopHome + "/lib/" + Tokens.TITAN_HADOOP_JOB_JAR).exists()) {
                    hadoopFileJar = titanHadoopHome + "/lib/" + Tokens.TITAN_HADOOP_JOB_JAR;
                    logger.info("Using the distribution Titan/Hadoop job jar: " + hadoopFileJar);
                }
            }
        } else {
            logger.info("Using the provided Titan/Hadoop job jar: " + hadoopFileJar);
        }
        if (null == hadoopFileJar)
            throw new IllegalStateException("The Titan/Hadoop job jar could not be found: " + Tokens.TITAN_HADOOP_JOB_JAR);

        if (getTitanConf().get(TitanHadoopConfiguration.PIPELINE_TRACK_PATHS))
            logger.warn("Path tracking is enabled for this Titan/Hadoop job (space and time expensive)");
        if (getTitanConf().get(TitanHadoopConfiguration.PIPELINE_TRACK_STATE))
            logger.warn("State tracking is enabled for this Titan/Hadoop job (full deletes not possible)");

        final FileSystem hdfs = FileSystem.get(this.graph.getConf());
        final String outputJobPrefix = this.graph.getOutputLocation().toString() + "/" + Tokens.JOB;
        hdfs.mkdirs(this.graph.getOutputLocation());

        //////// CHAINING JOBS TOGETHER

        for (int i = 0; i < this.jobs.size(); i++) {
            final Job job = this.jobs.get(i);
            ConfigurationUtil.copyValue(job.getConfiguration(), getTitanConf(), TitanHadoopConfiguration.PIPELINE_TRACK_PATHS);
            ConfigurationUtil.copyValue(job.getConfiguration(), getTitanConf(), TitanHadoopConfiguration.PIPELINE_TRACK_STATE);
            job.getConfiguration().set(MAPRED_JAR, hadoopFileJar);
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
            logger.info("Titan/Hadoop: Distributed Graph Processing with Hadoop");
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
            try {
                ((JobConfigurationFormat) (FormatTools.getBaseOutputFormatClass(job).newInstance())).updateJob(job);
            } catch (final Exception e) {
            }
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
                logger.error("Titan/Hadoop job error -- remaining MapReduce jobs have been canceled");
                return -1;
            }
        }
        return 0;
    }
}