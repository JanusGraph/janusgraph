/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/*
 * This is based on ContextFactory.java from hadoop-2.0.x sources.
 */

/**
 * Utility methods to allow applications to deal with inconsistencies between
 * MapReduce Context Objects API between Hadoop 1.x and 2.x.
 */
public class HadoopCompat
{

    private static final boolean useV21;

    private static final Constructor<?> JOB_CONTEXT_CONSTRUCTOR;
    private static final Constructor<?> TASK_CONTEXT_CONSTRUCTOR;
    private static final Constructor<?> MAP_CONTEXT_CONSTRUCTOR;
    private static final Constructor<?> GENERIC_COUNTER_CONSTRUCTOR;

    private static final Field READER_FIELD;
    private static final Field WRITER_FIELD;

    private static final Method GET_CONFIGURATION_METHOD;
    private static final Method PROGRESS_METHOD;

    static
    {
        boolean v21 = true;
        final String PACKAGE = "org.apache.hadoop.mapreduce";
        try
        {
            Class.forName(PACKAGE + ".task.JobContextImpl");
        } catch (ClassNotFoundException cnfe)
        {
            v21 = false;
        }
        useV21 = v21;
        Class<?> jobContextCls;
        Class<?> taskContextCls;
        Class<?> taskIOContextCls;
        Class<?> mapContextCls;
        Class<?> genericCounterCls;
        try
        {
            if (v21)
            {
                jobContextCls =
                        Class.forName(PACKAGE+".task.JobContextImpl");
                taskContextCls =
                        Class.forName(PACKAGE+".task.TaskAttemptContextImpl");
                taskIOContextCls =
                        Class.forName(PACKAGE+".task.TaskInputOutputContextImpl");
                mapContextCls = Class.forName(PACKAGE + ".task.MapContextImpl");
                genericCounterCls = Class.forName(PACKAGE+".counters.GenericCounter");
            }
            else
            {
                jobContextCls =
                        Class.forName(PACKAGE+".JobContext");
                taskContextCls =
                        Class.forName(PACKAGE+".TaskAttemptContext");
                taskIOContextCls =
                        Class.forName(PACKAGE+".TaskInputOutputContext");
                mapContextCls = Class.forName(PACKAGE + ".MapContext");
                genericCounterCls =
                        Class.forName("org.apache.hadoop.mapred.Counters$Counter");

            }
        } catch (ClassNotFoundException e)
        {
            throw new IllegalArgumentException("Can't find class", e);
        }
        try
        {
            JOB_CONTEXT_CONSTRUCTOR =
                    jobContextCls.getConstructor(Configuration.class, JobID.class);
            JOB_CONTEXT_CONSTRUCTOR.setAccessible(true);
            TASK_CONTEXT_CONSTRUCTOR =
                    taskContextCls.getConstructor(Configuration.class,
                            TaskAttemptID.class);
            TASK_CONTEXT_CONSTRUCTOR.setAccessible(true);
            GENERIC_COUNTER_CONSTRUCTOR =
                    genericCounterCls.getDeclaredConstructor(String.class,
                            String.class,
                            Long.TYPE);
            GENERIC_COUNTER_CONSTRUCTOR.setAccessible(true);

            if (useV21)
            {
                MAP_CONTEXT_CONSTRUCTOR =
                        mapContextCls.getDeclaredConstructor(Configuration.class,
                                TaskAttemptID.class,
                                RecordReader.class,
                                RecordWriter.class,
                                OutputCommitter.class,
                                StatusReporter.class,
                                InputSplit.class);
            }
            else
            {
                MAP_CONTEXT_CONSTRUCTOR =
                        mapContextCls.getConstructor(Configuration.class,
                                TaskAttemptID.class,
                                RecordReader.class,
                                RecordWriter.class,
                                OutputCommitter.class,
                                StatusReporter.class,
                                InputSplit.class);
            }
            MAP_CONTEXT_CONSTRUCTOR.setAccessible(true);
            READER_FIELD = mapContextCls.getDeclaredField("reader");
            READER_FIELD.setAccessible(true);
            WRITER_FIELD = taskIOContextCls.getDeclaredField("output");
            WRITER_FIELD.setAccessible(true);
            GET_CONFIGURATION_METHOD = Class.forName(PACKAGE+".JobContext")
                    .getMethod("getConfiguration");
            PROGRESS_METHOD = Class.forName(PACKAGE+".TaskAttemptContext")
                    .getMethod("progress");

        }
        catch (SecurityException e)
        {
            throw new IllegalArgumentException("Can't run constructor ", e);
        }
        catch (NoSuchMethodException e)
        {
            throw new IllegalArgumentException("Can't find constructor ", e);
        }
        catch (NoSuchFieldException e)
        {
            throw new IllegalArgumentException("Can't find field ", e);
        }
        catch (ClassNotFoundException e)
        {
            throw new IllegalArgumentException("Can't find class", e);
        }
    }

    private static Object newInstance(Constructor<?> constructor, Object...args)
    {
        try
        {
            return constructor.newInstance(args);
        }
        catch (InstantiationException e)
        {
            throw new IllegalArgumentException("Can't instantiate " + constructor, e);
        }
        catch (IllegalAccessException e)
        {
            throw new IllegalArgumentException("Can't instantiate " + constructor, e);
        }
        catch (InvocationTargetException e)
        {
            throw new IllegalArgumentException("Can't instantiate " + constructor, e);
        }
    }

    /**
     * Creates TaskAttempContext from a JobConf and jobId using the correct
     * constructor for based on Hadoop version.
     */
    public static TaskAttemptContext newTaskAttemptContext(
            Configuration conf, TaskAttemptID taskAttemptId) {
        return (TaskAttemptContext)
                newInstance(TASK_CONTEXT_CONSTRUCTOR, conf, taskAttemptId);
    }

    /**
     * Instantiates MapContext under Hadoop 1 and MapContextImpl under Hadoop 2.
     */
    public static MapContext newMapContext(Configuration conf,
                                           TaskAttemptID taskAttemptID,
                                           RecordReader recordReader,
                                           RecordWriter recordWriter,
                                           OutputCommitter outputCommitter,
                                           StatusReporter statusReporter,
                                           InputSplit inputSplit) {
        return (MapContext) newInstance(MAP_CONTEXT_CONSTRUCTOR,
                conf, taskAttemptID, recordReader, recordWriter, outputCommitter,
                statusReporter, inputSplit);
    }

    /**
     * Invokes a method and rethrows any exception as runtime excetpions.
     */
    private static Object invoke(Method method, Object obj, Object... args)
    {
        try
        {
            return method.invoke(obj, args);
        }
        catch (IllegalAccessException | InvocationTargetException e)
        {
            throw new IllegalArgumentException("Can't invoke method " + method.getName(), e);
        }
    }

    /**
     * Invoke getConfiguration() on JobContext. Works with both
     * Hadoop 1 and 2.
     */
    public static Configuration getConfiguration(JobContext context)
    {
        return (Configuration) invoke(GET_CONFIGURATION_METHOD, context);
    }
}
