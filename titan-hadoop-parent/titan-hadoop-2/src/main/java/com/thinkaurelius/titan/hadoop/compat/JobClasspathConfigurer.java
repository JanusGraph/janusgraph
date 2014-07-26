package com.thinkaurelius.titan.hadoop.compat;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

/**
 * Stores configuration data related to either the mapred jar or the
 * DistributedCache classpath. Provides methods to set this configuration data
 * on Job objects.
 */
public interface JobClasspathConfigurer {

    public void configure(Job job) throws IOException;
}
