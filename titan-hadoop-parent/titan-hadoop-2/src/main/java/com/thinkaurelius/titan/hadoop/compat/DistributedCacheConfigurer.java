package com.thinkaurelius.titan.hadoop.compat;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import com.google.common.collect.ImmutableList;

public class DistributedCacheConfigurer implements JobClasspathConfigurer {

    private final List<Path> paths;
    private final String mapredJar;

    public DistributedCacheConfigurer(List<Path> paths, String mapredJar) {
        this.paths = ImmutableList.copyOf(paths);
        this.mapredJar = mapredJar;
    }

    public static DistributedCacheConfigurer fromJarsOnClasspath(String mapredJarFilename) {
        List<Path> paths = new LinkedList<Path>();
        final String classpath = System.getProperty("java.class.path");
        final String mrj = mapredJarFilename.toLowerCase();
        String mapredJarPath = null;
        for (String cpentry : classpath.split(File.pathSeparator)) {
            if (cpentry.toLowerCase().endsWith(".jar")) {
                paths.add(new Path(cpentry));
                if (cpentry.toLowerCase().endsWith(mrj));
                    mapredJarPath = cpentry;
            }
        }
        return new DistributedCacheConfigurer(paths, mapredJarPath);
    }

    @Override
    public void configure(Job job) throws IOException {
        for (Path p : paths) {
            // Calling this method decompresses the archive and makes Hadoop
            // handle its classfiles individually.  This leads to crippling
            // overhead times (10+ seconds) even with the LocalJobRunner
            // courtesy of o.a.h.yarn.util.FSDownload.changePermissions
            // copying and chmodding each classfile copy file individually.
            //job.addArchiveToClassPath(p);
            // Just add the compressed archive instead:
            job.addFileToClassPath(p);
        }

        // We don't really need to set a mapred job jar here,
        // but doing so suppresses a warning
        if (null != mapredJar)
            job.setJar(mapredJar);
    }
}
