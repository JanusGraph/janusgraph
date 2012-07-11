package com.thinkaurelius.faunus.drivers;

import com.thinkaurelius.faunus.io.formats.json.JSONInputFormat;
import com.thinkaurelius.faunus.io.formats.json.JSONOutputFormat;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.algebra.ExceptEdgeLabels;
import com.thinkaurelius.faunus.mapreduce.algebra.Function;
import com.thinkaurelius.faunus.mapreduce.algebra.Identity;
import com.thinkaurelius.faunus.mapreduce.algebra.RetainEdgeLabels;
import groovy.lang.Closure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class Faunus extends Configured implements Tool {

    private final Class<? extends InputFormat> inputFormat;
    private final Class<? extends OutputFormat> outputFormat;
    private final Path outputPath;
    private Path inputPath;

    private List<Job> jobs = new ArrayList<Job>();

    public Faunus(Class<? extends InputFormat> inputFormat, Path inputPath, Class<? extends OutputFormat> outputFormat, Path outputPath) {
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
        this.outputPath = outputPath;
        this.inputPath = inputPath;
        this.setConf(new Configuration());
    }

    public Faunus _() throws IOException {
        final Job job = new Job(this.getConf(), "Faunus: " + Identity.class.getSimpleName());
        job.setMapperClass(Identity.Map.class);
        this.loadMapOnlyJobConfiguration(job);
        this.jobs.add(job);
        return this;
    }

    public Faunus retainEdgeLabels(final String... labels) throws IOException {
        final Configuration conf = this.getConf();
        conf.setStrings(RetainEdgeLabels.LABELS, labels);
        final Job job = new Job(conf, "Faunus: " + RetainEdgeLabels.class.getSimpleName());
        this.loadMapOnlyJobConfiguration(job);
        job.setMapperClass(RetainEdgeLabels.Map.class);
        this.jobs.add(job);
        return this;
    }

    public Faunus exceptEdgeLabels(final String... labels) throws IOException {
        final Configuration conf = this.getConf();
        conf.setStrings(ExceptEdgeLabels.LABELS, labels);
        final Job job = new Job(conf, "Faunus: " + ExceptEdgeLabels.class.getSimpleName());
        this.loadMapOnlyJobConfiguration(job);
        job.setMapperClass(ExceptEdgeLabels.Map.class);
        this.jobs.add(job);
        return this;
    }

/*    public Faunus vfilter(Closure closure) throws IOException {
        Configuration conf = this.getConf();
        final Job job = new Job(conf, "Faunus: " + VertexFunctionFilter.class.getSimpleName());
        
        job.setMapperClass(Identity.Map.class);


        this.jobs.add(job);
        return this;
    }
*/

    private void loadMapOnlyJobConfiguration(final Job job) {
        job.setJarByClass(Faunus.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(FaunusVertex.class);
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
        for (final Job job : this.jobs) {
            job.waitForCompletion(true);
            final FileSystem hdfs = FileSystem.get(job.getConfiguration());
            final String tempFile = job.getConfiguration().get("inputPath");
            if (null != tempFile)
                hdfs.delete(new Path(tempFile), true);
        }
        return 0;
    }

    private void composeJobs() throws IOException {
        if (this.jobs.size() == 0) {
            return;
        }

        String uuid = UUID.randomUUID().toString();
        final Job startJob = this.jobs.get(0);
        startJob.setInputFormatClass(this.inputFormat);

        FileInputFormat.setInputPaths(startJob, this.inputPath);
        if (this.jobs.size() > 1) {
            FileOutputFormat.setOutputPath(startJob, new Path(uuid));
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
                FileInputFormat.setInputPaths(midJob, new Path(uuid));
                midJob.getConfiguration().set("inputPath", uuid);
                uuid = UUID.randomUUID().toString();
                FileOutputFormat.setOutputPath(midJob, new Path(uuid));
            }
        }
        if (this.jobs.size() > 1) {
            final Job endJob = this.jobs.get(this.jobs.size() - 1);
            endJob.setInputFormatClass(SequenceFileInputFormat.class);
            endJob.setOutputFormatClass(this.outputFormat);
            endJob.getConfiguration().set("inputPath", uuid);
            FileInputFormat.setInputPaths(endJob, new Path(uuid));
            FileOutputFormat.setOutputPath(endJob, this.outputPath);
        }

        // TODO: If job fails, go through an delete intermediate files.
    }

    public static void main(String[] args) throws Exception {
        final Faunus faunus = new Faunus(JSONInputFormat.class, new Path(args[0]), JSONOutputFormat.class, new Path(args[1]));
        final GroovyScriptEngineImpl scriptEngine = new GroovyScriptEngineImpl();
        scriptEngine.put("V", faunus);
        scriptEngine.eval(args[3]);
        int result = ToolRunner.run(faunus, args);
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
