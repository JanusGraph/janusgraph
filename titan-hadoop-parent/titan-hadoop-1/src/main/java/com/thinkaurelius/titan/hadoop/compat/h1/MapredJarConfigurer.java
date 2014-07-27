package com.thinkaurelius.titan.hadoop.compat.h1;

import com.thinkaurelius.titan.hadoop.config.job.JobClasspathConfigurer;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class MapredJarConfigurer implements JobClasspathConfigurer {

    private final String mapredJar;

    public MapredJarConfigurer(String mapredJar) {
        this.mapredJar = mapredJar;
    }

    @Override
    public void configure(Job job) throws IOException {
        job.getConfiguration().set(Hadoop1Compat.CFG_JOB_JAR, mapredJar);
    }
}
