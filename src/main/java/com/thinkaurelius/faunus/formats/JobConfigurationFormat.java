package com.thinkaurelius.faunus.formats;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface JobConfigurationFormat {

    public void updateJob(Job job) throws InterruptedException, IOException;
}
