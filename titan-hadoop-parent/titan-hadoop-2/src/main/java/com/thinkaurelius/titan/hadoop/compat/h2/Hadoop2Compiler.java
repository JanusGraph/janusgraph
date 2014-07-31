package com.thinkaurelius.titan.hadoop.compat.h2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
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
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;
import com.thinkaurelius.titan.hadoop.config.HybridConfigured;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.config.job.JobClasspathConfigurer;
import com.thinkaurelius.titan.hadoop.config.job.JobClasspathConfigurers;
import com.thinkaurelius.titan.hadoop.formats.FormatTools;
import com.thinkaurelius.titan.hadoop.formats.JobConfigurationFormat;
import com.thinkaurelius.titan.hadoop.hdfs.NoSideEffectFilter;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Hadoop2Compiler extends HybridConfigured implements HadoopCompiler {

    private static final String MAPRED_COMPRESS_MAP_OUTPUT = "mapred.compress.map.output";
    private static final String MAPRED_MAP_OUTPUT_COMPRESSION_CODEC = "mapred.map.output.compression.codec";

    enum State {MAPPER, REDUCER, NONE}

    private static final String ARROW = " > ";
    private static final String MAPREDUCE_MAP_OUTPUT_COMPRESS = "mapreduce.map.output.compress";
    private static final String MAPREDUCE_MAP_OUTPUT_COMPRESS_CODEC = "mapreduce.map.output.compress.codec";

    public static final Logger logger = Logger.getLogger(Hadoop2Compiler.class);

    private HadoopGraph graph;

    protected final List<Job> jobs = new ArrayList<Job>();

    private State state = State.NONE;

    private static final Class<? extends InputFormat> INTERMEDIATE_INPUT_FORMAT = SequenceFileInputFormat.class;
    private static final Class<? extends OutputFormat> INTERMEDIATE_OUTPUT_FORMAT = SequenceFileOutputFormat.class;

    static final String JOB_JAR = "titan-hadoop-2-" + TitanConstants.VERSION + "-job.jar";

    private static final String MAPRED_JAR = "mapred.jar";

    public Hadoop2Compiler(final HadoopGraph graph) {
        this.graph = graph;
        this.setConf(new Configuration(this.graph.getConf()));
    }

    private String makeClassName(final Class klass) {
        return klass.getCanonicalName().replace(klass.getPackage().getName() + ".", "");
    }

    @Override
    public void addMapReduce(final Class<? extends Mapper> mapper,
                             final Class<? extends Reducer> combiner,
                             final Class<? extends Reducer> reducer,
                             final Class<? extends WritableComparable> mapOutputKey,
                             final Class<? extends WritableComparable> mapOutputValue,
                             final Class<? extends WritableComparable> reduceOutputKey,
                             final Class<? extends WritableComparable> reduceOutputValue,
                             final Configuration configuration) {
        this.addMapReduce(mapper, combiner, reducer, null, mapOutputKey, mapOutputValue, reduceOutputKey, reduceOutputValue, configuration);
    }

    @Override
    public void addMapReduce(final Class<? extends Mapper> mapper,
                             final Class<? extends Reducer> combiner,
                             final Class<? extends Reducer> reducer,
                             final Class<? extends WritableComparator> comparator,
                             final Class<? extends WritableComparable> mapOutputKey,
                             final Class<? extends WritableComparable> mapOutputValue,
                             final Class<? extends WritableComparable> reduceOutputKey,
                             final Class<? extends WritableComparable> reduceOutputValue,
                             final Configuration configuration) {
       try {
            final Job job;

            // Combine this.getConf() with the configuration argument (latter takes precedence)
            final Configuration mergedConf = new Configuration(this.getConf());
            final Iterator<Entry<String,String>> it = configuration.iterator();
            while (it.hasNext()) {
                Entry<String,String> ent = it.next();
                mergedConf.set(ent.getKey(), ent.getValue());
            }

            if (State.NONE == this.state || State.REDUCER == this.state) {
                // Set merged configuration for the new job
                //
                // This really does matter; just setting the config in
                // ChainMapper.addMapper and ChainReducer.setReducer invocations
                // below is not sufficient for some jobs that use a combiner.
                // For example, LinkMapReduce.Combiner expects to use custom
                // config keys like DIRECTION. Leaving out this step effectively
                // drops that combiner's custom keys and makes tests using
                // linkIn pipeline steps fail.
                job = Job.getInstance(mergedConf);
                job.setJobName(makeClassName(mapper) + ARROW + makeClassName(reducer));
                this.jobs.add(job);
            } else {
                job = this.jobs.get(this.jobs.size() - 1);
                job.setJobName(job.getJobName() + ARROW + makeClassName(mapper) + ARROW + makeClassName(reducer));
            }
            job.setNumReduceTasks(this.getConf().getInt("mapreduce.job.reduces", this.getConf().getInt("mapreduce.tasktracker.reduce.tasks.maximum", 1)));

            ChainMapper.addMapper(job, mapper, NullWritable.class, FaunusVertex.class, mapOutputKey, mapOutputValue, configuration);
            ChainReducer.setReducer(job, reducer, mapOutputKey, mapOutputValue, reduceOutputKey, reduceOutputValue, configuration);

            if (null != comparator)
                job.setSortComparatorClass(comparator);
            if (null != combiner)
                job.setCombinerClass(combiner);
            if (null == job.getConfiguration().get(MAPREDUCE_MAP_OUTPUT_COMPRESS, null))
                job.getConfiguration().setBoolean(MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
            if (null == job.getConfiguration().get(MAPREDUCE_MAP_OUTPUT_COMPRESS_CODEC, null))
                job.getConfiguration().setClass(MAPREDUCE_MAP_OUTPUT_COMPRESS_CODEC, DefaultCodec.class, CompressionCodec.class);
            this.state = State.REDUCER;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

    }

    @Override
    public void addMap(final Class<? extends Mapper> mapper,
                       final Class<? extends WritableComparable> mapOutputKey,
                       final Class<? extends WritableComparable> mapOutputValue,
                       final Configuration configuration) {
        try {
            final Job job;
            if (State.NONE == this.state) {
                job = Job.getInstance(this.getConf());
                job.setNumReduceTasks(0);
                job.setJobName(makeClassName(mapper));
                this.jobs.add(job);
            } else {
                job = this.jobs.get(this.jobs.size() - 1);
                job.setJobName(job.getJobName() + ARROW + makeClassName(mapper));
            }

            if (State.MAPPER == this.state || State.NONE == this.state) {
                ChainMapper.addMapper(job, mapper, NullWritable.class, FaunusVertex.class, mapOutputKey, mapOutputValue, configuration);
                this.state = State.MAPPER;
            } else {
                ChainReducer.addMapper(job, mapper, NullWritable.class, FaunusVertex.class, mapOutputKey, mapOutputValue, configuration);
                this.state = State.REDUCER;
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void completeSequence() {
        // noop
    }

    @Override
    public void composeJobs() throws IOException {

        if (this.jobs.size() == 0) {
            return;
        }

        if (getTitanConf().get(TitanHadoopConfiguration.PIPELINE_TRACK_PATHS))
            logger.warn("Path tracking is enabled for this Titan/Hadoop job (space and time expensive)");
        if (getTitanConf().get(TitanHadoopConfiguration.PIPELINE_TRACK_STATE))
            logger.warn("State tracking is enabled for this Titan/Hadoop job (full deletes not possible)");

        JobClasspathConfigurer cpConf = JobClasspathConfigurers.get(graph.getConf().get(MAPRED_JAR), JOB_JAR);

        // Create temporary job data directory on the filesystem
        Path tmpPath = graph.getJobDir();
        final FileSystem fs = FileSystem.get(graph.getConf());
        fs.mkdirs(tmpPath);
        logger.debug("Created " + tmpPath + " on filesystem " + fs);
        final String jobTmp = tmpPath.toString() + "/" + Tokens.JOB;
        logger.debug("Set jobDir=" + jobTmp);

        //////// CHAINING JOBS TOGETHER

        for (int i = 0; i < this.jobs.size(); i++) {
            final Job job = this.jobs.get(i);
            for (ConfigOption<Boolean> c : Arrays.asList(TitanHadoopConfiguration.PIPELINE_TRACK_PATHS, TitanHadoopConfiguration.PIPELINE_TRACK_STATE)) {
                ModifiableHadoopConfiguration jobFaunusConf = ModifiableHadoopConfiguration.of(job.getConfiguration());
                jobFaunusConf.set(c, getTitanConf().get(c));
            }
            SequenceFileOutputFormat.setOutputPath(job, new Path(jobTmp + "-" + i));
            cpConf.configure(job);

            // configure job inputs
            if (i == 0) {
                job.setInputFormatClass(this.graph.getGraphInputFormat());
                if (FileInputFormat.class.isAssignableFrom(this.graph.getGraphInputFormat())) {
                    FileInputFormat.setInputPaths(job, this.graph.getInputLocation());
                    FileInputFormat.setInputPathFilter(job, NoSideEffectFilter.class);
                }
            } else {
                job.setInputFormatClass(INTERMEDIATE_INPUT_FORMAT);
                FileInputFormat.setInputPaths(job, new Path(jobTmp + "-" + (i - 1)));
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

    @Override
    public int run(final String[] args) throws Exception {
        String script = null;
        boolean showHeader = true;

        if (args.length == 2) {
            script = args[0];
            showHeader = Boolean.valueOf(args[1]);
        }

        final FileSystem hdfs = FileSystem.get(this.getConf());
        if (null != graph.getJobDir() &&
            graph.getJobDirOverwrite() &&
            hdfs.exists(graph.getJobDir())) {
            hdfs.delete(graph.getJobDir(), true);
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

        final String jobTmp = graph.getJobDir().toString() + "/" + Tokens.JOB;

        for (int i = 0; i < this.jobs.size(); i++) {
            final Job job = this.jobs.get(i);
            try {
                ((JobConfigurationFormat) (FormatTools.getBaseOutputFormatClass(job).newInstance())).updateJob(job);
            } catch (final Exception e) {
            }
            logger.info("Executing job " + (i + 1) + " out of " + this.jobs.size() + ": " + job.getJobName());
            boolean success = job.waitForCompletion(true);
            if (i > 0) {
                Preconditions.checkNotNull(jobTmp);
                logger.debug("Cleaninng job data location: " + jobTmp + "-" + i);
                final Path path = new Path(jobTmp + "-" + (i - 1));
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
