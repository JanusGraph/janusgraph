package com.thinkaurelius.titan.hadoop.formats;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FormatTools {

    public static Class getBaseOutputFormatClass(final Job job) {
        try {
            if (LazyOutputFormat.class.isAssignableFrom(job.getOutputFormatClass())) {
                Class<OutputFormat> baseClass = (Class<OutputFormat>)
                        DEFAULT_COMPAT.getJobContextConfiguration(job).getClass(LazyOutputFormat.OUTPUT_FORMAT, null);
                return (null == baseClass) ? job.getOutputFormatClass() : baseClass;
            }
            return job.getOutputFormatClass();
        } catch (Exception e) {
            return null;
        }
    }
}
