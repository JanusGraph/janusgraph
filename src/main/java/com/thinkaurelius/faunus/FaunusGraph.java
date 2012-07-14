package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.json.JSONInputFormat;
import com.thinkaurelius.faunus.formats.json.JSONOutputFormat;
import com.thinkaurelius.faunus.mapreduce.steps.ExceptEdgeLabels;
import com.thinkaurelius.faunus.mapreduce.steps.Function;
import com.thinkaurelius.faunus.mapreduce.steps.Identity;
import com.thinkaurelius.faunus.mapreduce.steps.MultipleMapStep;
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
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusGraph extends Configured implements Tool {

    private final Class<? extends InputFormat> inputFormat;
    private final Class<? extends OutputFormat> outputFormat;
    private final Path outputPath;
    private final Path inputPath;
    private final String jobScript;

    private final List<Job> jobs = new ArrayList<Job>();
    private final List<Path> intermediateFiles = new ArrayList<Path>();

    private final List<String> mapClasses = new ArrayList<String>();

    public FaunusGraph V = this;

    public FaunusGraph(final Class<? extends InputFormat> inputFormat, final Path inputPath, final Class<? extends OutputFormat> outputFormat, final Path outputPath, final String jobScript) {
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
        this.outputPath = outputPath;
        this.inputPath = inputPath;
        this.jobScript = jobScript;
        this.setConf(new Configuration());
    }

    public FaunusGraph _() throws IOException {

        //mapClasses.add(Identity.Map.class.getName());

        final Job job = new Job(new Configuration(), this.jobScript);
        job.setMapperClass(Identity.Map.class);
        this.configureMapJob(job);
        this.jobs.add(job);
        return this;

    }

    public FaunusGraph retainEdgeLabels(final String... labels) throws IOException {

        //mapClasses.add(RetainEdgeLabels.Map.class.getName());
        final Configuration conf = new Configuration();
        conf.setStrings(RetainEdgeLabels.LABELS, labels);
        final Job job = new Job(conf, this.jobScript);
        this.configureMapJob(job);
        job.setMapperClass(RetainEdgeLabels.Map.class);
        this.jobs.add(job);
        return this;
    }

    public FaunusGraph exceptEdgeLabels(final String... labels) throws IOException {

        //mapClasses.add(ExceptEdgeLabels.Map.class.getName());
        final Configuration conf = new Configuration();
        conf.setStrings(ExceptEdgeLabels.LABELS, labels);
        final Job job = new Job(conf, this.jobScript);
        this.configureMapJob(job);
        job.setMapperClass(ExceptEdgeLabels.Map.class);
        this.jobs.add(job);
        return this;
    }

    /*public FaunusGraph done() throws IOException {
        final Configuration conf = new Configuration();
        conf.setStrings(MultipleMapStep.CLASSES, mapClasses.toArray(new String[mapClasses.size()]));

        final Job job = new Job(conf, this.jobScript);
        job.setMapperClass(MultipleMapStep.Map.class);
        this.configureMapJob(job);
        this.jobs.add(job);
        return this;
    }*/

    public FaunusGraph self(final boolean allow) throws IOException {
        final Configuration conf = new Configuration();
        conf.setBoolean(Self.ALLOW, allow);
        final Job job = new Job(conf, this.jobScript);
        this.configureMapJob(job);
        job.setMapperClass(Self.Map.class);
        this.jobs.add(job);
        return this;
    }

    public FaunusGraph transpose(final String label, final String newLabel) throws IOException {
        final Configuration conf = new Configuration();
        conf.set(Transpose.LABEL, label);
        conf.set(Transpose.NEW_LABEL, newLabel);
        final Job job = new Job(conf, this.jobScript);
        this.configureMapReduceJob(job);
        job.setMapperClass(Transpose.Map.class);
        job.setReducerClass(Transpose.Reduce.class);
        this.jobs.add(job);
        return this;
    }

    public FaunusGraph traverse(final Direction firstDirection, final String firstLabel, final Direction secondDirection, final String secondLabel, final String newLabel) throws IOException {
        final Configuration conf = new Configuration();
        conf.set(Traverse.FIRST_DIRECTION, firstDirection.toString());
        conf.set(Traverse.FIRST_LABEL, firstLabel);
        conf.set(Traverse.SECOND_DIRECTION, secondDirection.toString());
        conf.set(Traverse.SECOND_LABEL, secondLabel);
        conf.set(Traverse.NEW_LABEL, newLabel);
        final Job job = new Job(conf, this.jobScript);
        this.configureMapReduceJob(job);
        job.setMapperClass(Traverse.Map.class);
        job.setReducerClass(Traverse.Reduce.class);
        this.jobs.add(job);
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

    public int run(String[] args) throws Exception {
        if (Boolean.valueOf(args[2])) {
            FileSystem hdfs = FileSystem.get(this.getConf());
            Path path = new Path(args[1]);
            if (hdfs.exists(path)) {
                hdfs.delete(path, true);
            }
        }

        this.composeJobs();
        for (int i = 0; i < this.jobs.size(); i++) {
            final Job job = this.jobs.get(i);
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
                intermediateFiles.add(path);
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
                    FileInputFormat.setInputPaths(midJob, intermediateFiles.get(intermediateFiles.size() - 1));
                    final Path path = new Path(UUID.randomUUID().toString());
                    FileOutputFormat.setOutputPath(midJob, path);
                    intermediateFiles.add(path);
                }
            }
            if (this.jobs.size() > 1) {
                final Job endJob = this.jobs.get(this.jobs.size() - 1);
                endJob.setInputFormatClass(SequenceFileInputFormat.class);
                endJob.setOutputFormatClass(this.outputFormat);
                FileInputFormat.setInputPaths(endJob, intermediateFiles.get(intermediateFiles.size() - 1));
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
        scriptEngine.eval(args[3]);
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
