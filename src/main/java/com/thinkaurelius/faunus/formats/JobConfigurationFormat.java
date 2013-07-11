package com.thinkaurelius.faunus.formats;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * If an Input- or OutputFormat requires a dynamic configuration of the job at execution time, then a JobConfigurationFormat can be implemented.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface JobConfigurationFormat {

    public void updateJob(Job job) throws InterruptedException, IOException;
}
