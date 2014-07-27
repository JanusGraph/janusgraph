package com.thinkaurelius.titan.hadoop.config.job;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;

/**
 * Abstract base class for {@link com.thinkaurelius.titan.hadoop.config.job.JobClasspathConfigurer}
 * implementations that use Hadoop's distributed cache to store push classfiles to the cluster.
 */
public abstract class AbstractDistCacheConfigurer {

    private final Conf conf;

    public AbstractDistCacheConfigurer(List<Path> paths, String mapredJar) {
        this.conf = new Conf(paths, mapredJar);
    }

    public AbstractDistCacheConfigurer(String mapredJarFilename) {
        this.conf = configureByClasspath(mapredJarFilename);
    }

    public String getMapredJar() {
        return conf.mapredJar;
    }

    public ImmutableList<Path> getPaths() {
        return conf.paths;
    }

    private static Conf configureByClasspath(String mapredJarFilename) {
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
        return new Conf(paths, mapredJarPath);
    }

    private static class Conf {

        private final ImmutableList<Path> paths;
        private final String mapredJar;

        public Conf(List<Path> paths, String mapredJar) {
            this.paths = ImmutableList.copyOf(paths);
            this.mapredJar = mapredJar;
        }
    }
}