package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.json.JSONInputFormat;
import com.thinkaurelius.faunus.formats.json.JSONOutputFormat;
import com.thinkaurelius.faunus.mapreduce.steps.ExceptEdgeLabels;
import com.thinkaurelius.faunus.mapreduce.steps.Function;
import com.thinkaurelius.faunus.mapreduce.steps.Identity;
import com.thinkaurelius.faunus.mapreduce.steps.MapReduceSequence;
import com.thinkaurelius.faunus.mapreduce.steps.MapSequence;
import com.thinkaurelius.faunus.mapreduce.steps.RetainEdgeLabels;
import com.thinkaurelius.faunus.mapreduce.steps.Self;
import com.thinkaurelius.faunus.mapreduce.steps.Transpose;
import com.thinkaurelius.faunus.mapreduce.steps.Traverse;
import com.thinkaurelius.faunus.util.TaggedHolder;
import com.tinkerpop.blueprints.Direction;
import groovy.lang.Closure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusGraph extends Configured implements Tool {

    private final Logger logger = Logger.getLogger(FaunusGraph.class);

    private final Class<? extends InputFormat> inputFormat;
    private final Class<? extends OutputFormat> outputFormat;
    private final Path outputPath;
    private final Path inputPath;
    private final String jobScript;

    private final List<Job> jobs = new ArrayList<Job>();
    private final List<Path> intermediateFiles = new ArrayList<Path>();

    private Configuration mapSequenceConfiguration = new Configuration();
    private final List<Class> mapSequenceClasses = new ArrayList<Class>();
    private Class mapRClass = null;
    private Class reduceClass = null;

    public FaunusGraph V = this;

    public FaunusGraph(final Class<? extends InputFormat> inputFormat, final Path inputPath, final Class<? extends OutputFormat> outputFormat, final Path outputPath, final String jobScript) {
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
        this.outputPath = outputPath;
        this.inputPath = inputPath;
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

    public FaunusGraph _() throws IOException {
        this.mapSequenceClasses.add(Identity.Map.class);
        return this;
    }

    public FaunusGraph retainEdgeLabels(final String... labels) throws IOException {
        this.mapSequenceConfiguration.setStrings(RetainEdgeLabels.LABELS + "-" + this.mapSequenceClasses.size(), labels);
        this.mapSequenceClasses.add(RetainEdgeLabels.Map.class);
        return this;
    }

    public FaunusGraph exceptEdgeLabels(final String... labels) throws IOException {
        this.mapSequenceConfiguration.setStrings(ExceptEdgeLabels.LABELS + "-" + this.mapSequenceClasses.size(), labels);
        this.mapSequenceClasses.add(ExceptEdgeLabels.Map.class);
        return this;
    }

    private FaunusGraph completeSequence() throws IOException {
        if (this.mapRClass != null && this.reduceClass != null) {
            this.mapSequenceConfiguration.set(MapReduceSequence.MAPR_CLASS, this.mapRClass.getName());
            this.mapSequenceConfiguration.set(MapReduceSequence.REDUCE_CLASS, this.reduceClass.getName());
            if (this.mapSequenceClasses.size() > 0) {
                this.mapSequenceConfiguration.setStrings(MapSequence.MAP_CLASSES, toStringMapSequenceClasses());
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
        this.mapSequenceConfiguration = new Configuration();
        return this;
    }

    public FaunusGraph self(final boolean allow) throws IOException {
        this.mapSequenceConfiguration.setBoolean(Self.ALLOW + "-" + this.mapSequenceClasses.size(), allow);
        this.mapSequenceClasses.add(Self.Map.class);
        return this;
    }

    public FaunusGraph transpose(final String label, final String newLabel) throws IOException {

        this.mapSequenceConfiguration.set(Transpose.LABEL, label);
        this.mapSequenceConfiguration.set(Transpose.NEW_LABEL, newLabel);
        this.mapRClass = Transpose.Map.class;
        this.reduceClass = Transpose.Reduce.class;
        this.completeSequence();
        return this;
    }

    public FaunusGraph traverse(final Direction firstDirection, final String firstLabel, final Direction secondDirection, final String secondLabel, final String newLabel) throws IOException {

        this.mapSequenceConfiguration.set(Traverse.FIRST_DIRECTION, firstDirection.toString());
        this.mapSequenceConfiguration.set(Traverse.FIRST_LABEL, firstLabel);
        this.mapSequenceConfiguration.set(Traverse.SECOND_DIRECTION, secondDirection.toString());
        this.mapSequenceConfiguration.set(Traverse.SECOND_LABEL, secondLabel);
        this.mapSequenceConfiguration.set(Traverse.NEW_LABEL, newLabel);
        this.mapRClass = Traverse.Map.class;
        this.reduceClass = Traverse.Reduce.class;
        this.completeSequence();
        return this;
    }


    public FaunusGraph V() {
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
        job.setMapOutputValueClass(TaggedHolder.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FaunusVertex.class);
    }

    /*private static Map<String, String> configurationToMap(final Configuration configuration) {
        final Map<String, String> map = new HashMap<String, String>();
        for (Map.Entry<String, String> entry : configuration) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }*/

    public int run(String[] args) throws Exception {
        if (Boolean.valueOf(args[2])) {
            FileSystem hdfs = FileSystem.get(this.getConf());
            Path path = new Path(args[1]);
            if (hdfs.exists(path)) {
                hdfs.delete(path, true);
            }
        }
        logger.info("         \\,,,/");
        logger.info("         (o o)");
        logger.info("-----oOOo-(_)-oOOo-----");
        logger.info("Generating job chain: " + this.jobScript);
        this.composeJobs();
        for (int i = 0; i < this.jobs.size(); i++) {
            final Job job = this.jobs.get(i);
            logger.info("Executing job " + (i + 1) + " out of " + this.jobs.size() + ": " + job.getJobName());
            //logger.info("\tJob configuration: " + FaunusGraph.configurationToMap(job.getConfiguration()));
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

            FileInputFormat.setInputPaths(startJob, this.inputPath);
            if (this.jobs.size() > 1) {
                final Path path = new Path(UUID.randomUUID().toString());
                FileOutputFormat.setOutputPath(startJob, path);
                this.intermediateFiles.add(path);
                startJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            } else {
                FileOutputFormat.setOutputPath(startJob, this.outputPath);
                startJob.setOutputFormatClass(this.outputFormat);

            }

            if (this.jobs.size() > 2) {
                for (int i = 1; i < this.jobs.size() - 1; i++) {
                    final Job midJob = this.jobs.get(i);
                    midJob.setInputFormatClass(SequenceFileInputFormat.class);
                    midJob.setOutputFormatClass(SequenceFileOutputFormat.class);
                    FileInputFormat.setInputPaths(midJob, this.intermediateFiles.get(this.intermediateFiles.size() - 1));
                    final Path path = new Path(UUID.randomUUID().toString());
                    FileOutputFormat.setOutputPath(midJob, path);
                    this.intermediateFiles.add(path);
                }
            }
            if (this.jobs.size() > 1) {
                final Job endJob = this.jobs.get(this.jobs.size() - 1);
                endJob.setInputFormatClass(SequenceFileInputFormat.class);
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
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Faunus: A Library of Graph-Based Hadoop Tools");
            System.out.println("Usage:");
            System.out.println("  arg1: input file (string)");
            System.out.println("  arg2: output file (string)");
            System.out.println("  arg3: overwrite existing output file (boolean)");
            System.out.println("  arg4: faunus script (string)");
            System.out.println("        g.V.step().step()...");
            System.exit(-1);
        }
        final FaunusGraph faunusGraph = new FaunusGraph(JSONInputFormat.class, new Path(args[0]), JSONOutputFormat.class, new Path(args[1]), args[3]);
        final GroovyScriptEngineImpl scriptEngine = new GroovyScriptEngineImpl();
        scriptEngine.eval("IN = " + com.tinkerpop.blueprints.Direction.class.getName() + ".IN");
        scriptEngine.eval("OUT =" + com.tinkerpop.blueprints.Direction.class.getName() + ".OUT");
        scriptEngine.put("g", faunusGraph);
        ((FaunusGraph) scriptEngine.eval(args[3])).completeSequence();
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
