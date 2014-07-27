package com.thinkaurelius.titan.hadoop.compat.h1;

import com.thinkaurelius.titan.hadoop.config.job.AbstractDistCacheConfigurer;
import com.thinkaurelius.titan.hadoop.config.job.JobClasspathConfigurer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class DistCacheConfigurer extends AbstractDistCacheConfigurer implements JobClasspathConfigurer {

    public DistCacheConfigurer(String mapredJarFilename) {
        super(mapredJarFilename);
    }

    @Override
    public void configure(Job job) throws IOException {

        for (Path p : getPaths()) {
            Configuration conf = job.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            DistributedCache.addFileToClassPath(p, conf, fs);
        }

        // We don't really need to set a mapred job jar here,
        // but doing so suppresses a warning
        String mj = getMapredJar();
        if (null != mj)
            job.getConfiguration().set(Hadoop1Compat.CFG_JOB_JAR, mj);
    }
}
