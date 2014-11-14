package com.thinkaurelius.titan.hadoop.scan;

import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.cassandra.CassandraBinaryInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.Map;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

/**
 * Utility class to construct and submit Hadoop Jobs that execute a {@link HadoopScanMapper}.
 */
public class HadoopScanRunner {

    public static ScanMetrics run(ScanJob j, Configuration jobConf, ConfigNamespace jobConfRoot, String jobConfRootName) throws IOException, ClassNotFoundException, InterruptedException {
        ModifiableHadoopConfiguration scanConf = ModifiableHadoopConfiguration.of(TitanHadoopConfiguration.SCAN_NS,
                new org.apache.hadoop.conf.Configuration());

        ModifiableConfiguration hadoopJobConf = ModifiableHadoopConfiguration.subset(jobConfRoot, TitanHadoopConfiguration.JOB_CONFIG_KEYS, scanConf);

        Map<String, Object> jobConfMap = jobConf.getSubset(jobConfRoot);

        // TODO This is super ugly
        for (Map.Entry<String, Object> jobConfEntry : jobConfMap.entrySet()) {
            hadoopJobConf.set((ConfigOption) ConfigElement.parse(jobConfRoot, jobConfEntry.getKey()).element, jobConfEntry.getValue());
        }

        Class.forName(j.getClass().getName());

        scanConf.set(TitanHadoopConfiguration.JOB_CLASS, j.getClass().getName());
        scanConf.set(TitanHadoopConfiguration.JOB_CONFIG_ROOT, jobConfRootName); // TODO this is also terrible

        scanConf.getHadoopConfiguration().set("cassandra.input.partitioner.class","org.apache.cassandra.dht.Murmur3Partitioner");

        // TODO the following is probably not compatible across Hadoop 1/2
        Job job = Job.getInstance(scanConf.getHadoopConfiguration());

        job.setJarByClass(HadoopScanMapper.class);
        job.setJobName(HadoopScanMapper.class.getSimpleName() + "[" + j + "]");
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        job.setMapperClass(HadoopScanMapper.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setInputFormatClass(CassandraBinaryInputFormat.class);

        boolean success = job.waitForCompletion(true);

        if (!success) {
            String f;
            try {
                // Just in case one of Job's methods throws an exception
                f = String.format("MapReduce JobID %s terminated in state %s",
                        job.getJobID().toString(), job.getStatus().getState().name());
            } catch (Throwable t) {
                f = "Job failed (see MapReduce logs for more information)";
            }
            throw new IOException(f);
        } else {
            return DEFAULT_COMPAT.getMetrics(job.getCounters());
        }
    }
}
