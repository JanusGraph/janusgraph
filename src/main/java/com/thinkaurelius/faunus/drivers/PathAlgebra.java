package com.thinkaurelius.faunus.drivers;

import com.thinkaurelius.faunus.io.formats.json.JSONInputFormat;
import com.thinkaurelius.faunus.io.formats.json.JSONOutputFormat;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.algebra.Identity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathAlgebra extends Configured implements Tool {

    /* public int run(String[] args) throws Exception {
       Configuration config = this.getConf();
       config.setStrings(LabelFilter.LABELS, "knows");
       config.setStrings(Traverse.LABELS, "knows", "created");
       Job job = new Job(config, "Faunus: Path Algebra");
       job.setJarByClass(PathAlgebra.class);

       String uuid = UUID.randomUUID().toString();
       Path in = new Path(args[0]);
       Path out = new Path(uuid);
       FileInputFormat.setInputPaths(job, in);
       FileOutputFormat.setOutputPath(job, out);

       job.setMapperClass(Traverse.Map1.class);
       job.setReducerClass(Traverse.Reduce1.class);
       //job.setNumReduceTasks(0);
       job.setInputFormatClass(JSONInputFormat.class);
       job.setOutputFormatClass(SequenceFileOutputFormat.class);
       job.setMapOutputKeyClass(LongWritable.class);
       job.setMapOutputValueClass(TaggedHolder.class);
       job.setOutputKeyClass(LongWritable.class);
       job.setOutputValueClass(Holder.class);
       job.waitForCompletion(true);

       /////////////////

       Job job2 = new Job(config, "Faunus: Path Algebra 2");
       job2.setJarByClass(PathAlgebra.class);

       Path in2 = new Path(uuid);
       Path out2 = new Path(args[1]);
       FileInputFormat.setInputPaths(job2, in2);
       FileOutputFormat.setOutputPath(job2, out2);

       job2.setMapperClass(Traverse.Map2.class);
       job2.setReducerClass(Traverse.Reduce2.class);
       job2.setInputFormatClass(SequenceFileInputFormat.class);
       job2.setOutputFormatClass(JSONOutputFormat.class);
       job2.setOutputKeyClass(LongWritable.class);
       job2.setOutputValueClass(Holder.class);
       job2.waitForCompletion(true);

       out.getFileSystem(config).delete(out, true);

       return 0;
   } */

    public int run(String[] args) throws Exception {
        Configuration config = this.getConf();
        //config.setStrings(LabelFilter.LABELS, "knows");
        //config.setStrings(Traverse.LABELS, "knows", "created");
        Job job = new Job(config, "Faunus: Path Algebra");
        job.setJarByClass(PathAlgebra.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(Identity.Map.class);
        //job.setNumReduceTasks(0);
        job.setInputFormatClass(JSONInputFormat.class);
        job.setOutputFormatClass(JSONOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(FaunusVertex.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FaunusVertex.class);
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new PathAlgebra(), args);
        System.exit(result);
    }
}
