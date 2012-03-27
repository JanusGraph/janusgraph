package com.thinkaurelius.faunus.drivers;

import com.thinkaurelius.faunus.io.formats.json.FaunusJSONInputFormat;
import com.thinkaurelius.faunus.io.formats.json.FaunusJSONOutputFormat;
import com.thinkaurelius.faunus.io.graph.util.ElementHolder;
import com.thinkaurelius.faunus.mapreduce.algebra.LabelFilter;
import com.thinkaurelius.faunus.mapreduce.algebra.Transpose;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathAlgebra extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration config = this.getConf();
        config.setStrings(LabelFilter.LABELS_PROPERTY, "knows");
        Job job = new Job(config, "Faunus: Path Algebra");
        job.setJarByClass(PathAlgebra.class);


        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(Transpose.Map.class);
        job.setReducerClass(Transpose.Reduce.class);
        //job.setNumReduceTasks(0);
        job.setInputFormatClass(FaunusJSONInputFormat.class);
        job.setOutputFormatClass(FaunusJSONOutputFormat.class);

        // mapper output
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(ElementHolder.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new PathAlgebra(), args);
        System.exit(result);
    }
}
