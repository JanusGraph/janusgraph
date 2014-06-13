package com.thinkaurelius.titan.hadoop.mapreduce;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.hadoop.HadoopGraph;

public class HadoopCompatLoader {

    private static final Logger log =
            LoggerFactory.getLogger(HadoopCompatLoader.class);

    public static HadoopCompiler getCompilerFor(HadoopGraph g) {
        String ver = VersionInfo.getVersion();

        final String className;
        if (ver.startsWith("1.")) {
            className = "com.thinkaurelius.titan.hadoop.mapreduce.Hadoop1Compiler";
        } else {
            className = "com.thinkaurelius.titan.hadoop.mapreduce.Hadoop2Compiler";
        }

        try {
            Constructor<?> ctor = Class.forName(className).getConstructor(HadoopGraph.class);
            return (HadoopCompiler)ctor.newInstance(g);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public static HadoopConfigKeys getConfigKeys() {
        String ver = VersionInfo.getVersion();

        final String className;
        if (ver.startsWith("1.")) {
            className = "com.thinkaurelius.titan.hadoop.mapreduce.Hadoop1ConfigKeys";
        } else {
            className = "com.thinkaurelius.titan.hadoop.mapreduce.Hadoop2ConfigKeys";
        }

        try {
            return (HadoopConfigKeys)Class.forName(className).newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
