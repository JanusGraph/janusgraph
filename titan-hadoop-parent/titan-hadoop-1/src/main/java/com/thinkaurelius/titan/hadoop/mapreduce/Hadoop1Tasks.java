package com.thinkaurelius.titan.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class Hadoop1Tasks implements HadoopTasks {
    public TaskAttemptContext newTask(Configuration c, TaskAttemptID t) {
        return new TaskAttemptContext(c, t);
    }
}
