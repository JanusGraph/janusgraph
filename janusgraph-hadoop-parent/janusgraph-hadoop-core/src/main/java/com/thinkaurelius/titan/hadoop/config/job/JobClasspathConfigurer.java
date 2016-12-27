package com.thinkaurelius.titan.hadoop.config.job;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

/**
 * Configures a Job a mapreduce jar and/or additional classpath elements hosted
 * in the Hadoop DistributedCache.
 */
public interface JobClasspathConfigurer {

    public void configure(Job job) throws IOException;
}
