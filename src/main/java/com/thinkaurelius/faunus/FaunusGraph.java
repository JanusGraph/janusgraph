package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.graphson.GraphSONInputFormat;
import com.thinkaurelius.faunus.formats.titan.TitanCassandraInputFormat;
import com.thinkaurelius.faunus.mapreduce.Function;
import com.thinkaurelius.faunus.mapreduce.MapReduceSequence;
import com.thinkaurelius.faunus.mapreduce.MapSequence;
import com.thinkaurelius.faunus.mapreduce.derivations.EdgeDirectionFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.EdgeLabelFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.Identity;
import com.thinkaurelius.faunus.mapreduce.derivations.PropertyFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.EdgePropertyValueFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.VertexPropertyValueFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.Self;
import com.thinkaurelius.faunus.mapreduce.derivations.Transpose;
import com.thinkaurelius.faunus.mapreduce.derivations.Traverse;
import com.thinkaurelius.faunus.mapreduce.statistics.AdjacentVertexProperties;
import com.thinkaurelius.faunus.mapreduce.statistics.DegreeDistribution;
import com.thinkaurelius.faunus.mapreduce.statistics.EdgeLabelDistribution;
import com.thinkaurelius.faunus.mapreduce.statistics.PropertyDistribution;
import com.thinkaurelius.faunus.mapreduce.statistics.SortedVertexDegree;
import com.thinkaurelius.faunus.mapreduce.statistics.VertexDegree;
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
    private final String jobScript;

    private final List<Job> jobs = new ArrayList<Job>();
    private final List<Path> intermediateFiles = new ArrayList<Path>();

    private Configuration mapSequenceConfiguration = new Configuration();
    private final List<Class> mapSequenceClasses = new ArrayList<Class>();
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

    public FaunusGraph propertyValueFilter(final Class<? extends Element> klass, final String key, final Query.Compare compare, final Object value) throws IOException {
        if (klass.equals(Vertex.class)) {
            this.mapSequenceConfiguration.set(VertexPropertyValueFilter.KEY, key);
            this.mapSequenceConfiguration.set(VertexPropertyValueFilter.COMPARE, compare.name());
            this.mapSequenceConfiguration.set(VertexPropertyValueFilter.VALUE, value.toString());
            if (value instanceof String) {
                this.mapSequenceConfiguration.setClass(VertexPropertyValueFilter.VALUE_CLASS, String.class, String.class);
            } else if (value instanceof Boolean) {
                this.mapSequenceConfiguration.setClass(VertexPropertyValueFilter.VALUE_CLASS, Boolean.class, Boolean.class);
            } else if (value instanceof Number) {
                this.mapSequenceConfiguration.setClass(VertexPropertyValueFilter.VALUE_CLASS, Number.class, Number.class);
            }
            this.mapRClass = VertexPropertyValueFilter.Map.class;
            this.reduceClass = VertexPropertyValueFilter.Reduce.class;
            this.completeSequence();
        } else {
            this.mapSequenceConfiguration.set(EdgePropertyValueFilter.KEY + "-" + this.mapSequenceClasses.size(), key);
            this.mapSequenceConfiguration.set(EdgePropertyValueFilter.COMPARE + "-" + this.mapSequenceClasses.size(), compare.name());
            this.mapSequenceConfiguration.set(EdgePropertyValueFilter.VALUE + "-" + this.mapSequenceClasses.size(), value.toString());
            if (value instanceof String) {
                this.mapSequenceConfiguration.setClass(EdgePropertyValueFilter.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), String.class, String.class);
            } else if (value instanceof Boolean) {
                this.mapSequenceConfiguration.setClass(EdgePropertyValueFilter.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), Boolean.class, Boolean.class);
            } else if (value instanceof Number) {
                this.mapSequenceConfiguration.setClass(EdgePropertyValueFilter.VALUE_CLASS + "-" + this.mapSequenceClasses.size(), Number.class, Number.class);
            }
            this.mapSequenceClasses.add(EdgePropertyValueFilter.Map.class);
        }
        return this;
    }

    public FaunusGraph edgeLabelFilter(final Tokens.Action action, final String... labels) {
        this.mapSequenceConfiguration.setStrings(EdgeLabelFilter.LABELS + "-" + this.mapSequenceClasses.size(), labels);
        this.mapSequenceConfiguration.set(EdgeLabelFilter.ACTION + "-" + this.mapSequenceClasses.size(), action.name());
        this.mapSequenceClasses.add(EdgeLabelFilter.Map.class);
        return this;
    }

    public FaunusGraph edgeDirectionFilter(final Direction direction) {
        this.mapSequenceConfiguration.set(EdgeDirectionFilter.DIRECTION + "-" + this.mapSequenceClasses.size(), direction.name());
        this.mapSequenceClasses.add(EdgeDirectionFilter.Map.class);
        return this;
    }

    public FaunusGraph self(final Tokens.Action action, final String labels) {
        this.mapSequenceConfiguration.set(Self.ACTION + "-" + this.mapSequenceClasses.size(), action.name());
        this.mapSequenceConfiguration.setStrings(Self.LABELS + "-" + this.mapSequenceClasses.size(), labels);
        this.mapSequenceClasses.add(Self.Map.class);
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

    ////  STATISTICS

    public FaunusGraph adjacentVertexProperties(final String property) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(AdjacentVertexProperties.PROPERTY, property);
        final Job job1 = new Job(conf, AdjacentVertexProperties.class.getCanonicalName() + ":part-1");
        job1.setMapperClass(AdjacentVertexProperties.Map.class);
        job1.setReducerClass(AdjacentVertexProperties.Reduce.class);
        job1.setJarByClass(FaunusGraph.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Holder.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        this.jobs.add(job1);

        // todo: aggregate or not?

        final Job job2 = new Job(new Configuration(), AdjacentVertexProperties.class.getCanonicalName() + ":part-2");
        job2.setMapperClass(AdjacentVertexProperties.Map2.class);
        job2.setCombinerClass(AdjacentVertexProperties.Reduce2.class);
        job2.setReducerClass(AdjacentVertexProperties.Reduce2.class);
        job2.setJarByClass(FaunusGraph.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        this.jobs.add(job2);

        this.outputFormat = this.statisticsOutputFormat;
        return this;


    }

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

    public FaunusGraph vertexDegree(final String property, final Tokens.Order order, final Direction direction, final String... labels) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(SortedVertexDegree.PROPERTY, property);
        conf.set(SortedVertexDegree.DIRECTION, direction.name());
        conf.setStrings(SortedVertexDegree.LABELS, labels);
        conf.set(SortedVertexDegree.ORDER, order.name());
        final Job job = new Job(conf, SortedVertexDegree.class.getCanonicalName());
        job.setMapperClass(SortedVertexDegree.Map.class);
        job.setReducerClass(SortedVertexDegree.Reduce.class);
        job.setJarByClass(FaunusGraph.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FaunusVertex.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
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
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        this.outputFormat = this.statisticsOutputFormat;
        this.jobs.add(job);
        return this;
    }

    public FaunusGraph propertyDistribution(final Class<? extends Element> klass, final String property) throws IOException {
        this.completeSequence();
        Configuration conf = new Configuration();
        conf.set(PropertyDistribution.CLASS, klass.getName());
        conf.set(PropertyDistribution.PROPERTY, property);
        final Job job = new Job(conf, EdgeLabelDistribution.class.getCanonicalName());
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

    /*private static Map<String, String> configurationToMap(final Configuration configuration) {
        final Map<String, String> map = new HashMap<String, String>();
        for (Map.Entry<String, String> entry : configuration) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }*/


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

        // TODO: should we do this? ---- Add Faunus properties to all the jobs
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
