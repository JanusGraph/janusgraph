package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.mapreduce.operators.DegreeDistribution;
import com.thinkaurelius.faunus.mapreduce.operators.EdgeLabelDistribution;
import com.thinkaurelius.faunus.mapreduce.operators.VertexDegree;
import com.thinkaurelius.faunus.mapreduce.steps.EdgeLabelFilter;
import com.thinkaurelius.faunus.mapreduce.steps.Function;
import com.thinkaurelius.faunus.mapreduce.steps.Identity;
import com.thinkaurelius.faunus.mapreduce.steps.MapReduceSequence;
import com.thinkaurelius.faunus.mapreduce.steps.MapSequence;
import com.thinkaurelius.faunus.mapreduce.steps.PropertyFilter;
import com.thinkaurelius.faunus.mapreduce.steps.Self;
import com.thinkaurelius.faunus.mapreduce.steps.Transpose;
import com.thinkaurelius.faunus.mapreduce.steps.Traverse;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import groovy.lang.Closure;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
    private final Path inputPath;
    private final String jobScript;

    private final List<Job> jobs = new ArrayList<Job>();
    private final List<Path> intermediateFiles = new ArrayList<Path>();

    private Configuration mapSequenceConfiguration = new Configuration();
    private final List<Class> mapSequenceClasses = new ArrayList<Class>();
    private Class mapRClass = null;
    private Class reduceClass = null;

    public FaunusGraph V = this;

    public FaunusGraph(final String jobScript, final Configuration conf) throws ClassNotFoundException {
        this.configuration = conf;
        this.inputFormat = (Class<? extends InputFormat>) Class.forName(this.configuration.get(Tokens.GRAPH_INPUT_FORMAT_CLASS));
        this.inputPath = new Path(this.configuration.get(Tokens.GRAPH_INPUT_LOCATION));
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

    public FaunusGraph _() throws IOException {
        this.mapSequenceClasses.add(Identity.Map.class);
        return this;
    }

    public FaunusGraph propertyFilter(final Tokens.Action action, final Class<? extends Element> klass, final String... keys) {
        this.mapSequenceConfiguration.set(PropertyFilter.ACTION + "-" + this.mapSequenceClasses.size(), action.name());
        this.mapSequenceConfiguration.set(PropertyFilter.CLASS + "-" + this.mapSequenceClasses.size(), klass.getName());
        this.mapSequenceConfiguration.setStrings(PropertyFilter.KEYS + "-" + this.mapSequenceClasses.size(), keys);
        this.mapSequenceClasses.add(PropertyFilter.Map.class);
        return this;
    }

    public FaunusGraph edgeLabelFilter(final Tokens.Action action, final String... labels) throws IOException {
        this.mapSequenceConfiguration.setStrings(EdgeLabelFilter.LABELS + "-" + this.mapSequenceClasses.size(), labels);
        this.mapSequenceConfiguration.set(EdgeLabelFilter.ACTION + "-" + this.mapSequenceClasses.size(), action.name());
        this.mapSequenceClasses.add(EdgeLabelFilter.Map.class);
        return this;
    }

    public FaunusGraph self(final Tokens.Action action) throws IOException {
        this.mapSequenceConfiguration.set(Self.ACTION + "-" + this.mapSequenceClasses.size(), action.name());
        this.mapSequenceClasses.add(Self.Map.class);
        return this;
    }

    public FaunusGraph transpose(final String label, final String newLabel, final Tokens.Action action) throws IOException {
        this.mapSequenceConfiguration.set(Transpose.LABEL, label);
        this.mapSequenceConfiguration.set(Transpose.NEW_LABEL, newLabel);
        this.mapSequenceConfiguration.set(Transpose.ACTION, action.name());
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

    ////

    public FaunusGraph vertexDegree(final String property, final Direction direction, final String... labels) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(VertexDegree.PROPERTY, property);
        conf.set(VertexDegree.DIRECTION, direction.name());
        conf.setStrings(VertexDegree.LABELS, labels);
        final Job job = new Job(conf, VertexDegree.class.getCanonicalName());
        job.setMapperClass(VertexDegree.Map.class);
        job.setJarByClass(FaunusGraph.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
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
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        this.outputFormat = this.statisticsOutputFormat;
        this.jobs.add(job);
        return this;
    }

    public FaunusGraph edgeLabelDistribution(final Direction direction) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(EdgeLabelDistribution.DIRECTION, direction.name());
        final Job job = new Job(conf, EdgeLabelDistribution.class.getCanonicalName());
        job.setMapperClass(EdgeLabelDistribution.Map.class);
        job.setReducerClass(EdgeLabelDistribution.Reduce.class);
        job.setCombinerClass(EdgeLabelDistribution.Reduce.class);
        job.setJarByClass(FaunusGraph.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        this.outputFormat = this.statisticsOutputFormat;
        this.jobs.add(job);
        return this;
    }

    private static Map<String, String> configurationToMap(final Configuration configuration) {
        final Map<String, String> map = new HashMap<String, String>();
        for (Map.Entry<String, String> entry : configuration) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
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
        logger.info("      .'   /\\     )  Faunus: A Library of Graph-Based Hadoop Tools");
        logger.info("   .-'  .'| \\  \\__");
        logger.info(" .'  __(  \\  '`(()");
        logger.info("/_.'`  `.  |    )(");
        logger.info("         \\ |");
        logger.info("          |/");
        //logger.info("Faunus configuration: " + this.configuration);
        logger.info("Generating job chain: " + this.jobScript);
        this.composeJobs();
        logger.info("Compiled to " + this.jobs.size() + " MapReduce job(s)");

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

        // Add Faunus properties to all the jobs
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
            System.out.println("  arg1: faunus configuration location (optional)");
            System.out.println("  arg2: faunus script: g.V.step().step()...");
            System.exit(-1);
        }

        final String script;
        final Properties properties = new Properties();
        if (args.length == 1) {
            script = args[0];
            properties.load(new FileInputStream("bin/faunus.properties"));
        } else {
            script = args[1];
            properties.load(new FileInputStream(args[0]));
        }
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
