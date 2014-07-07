package com.thinkaurelius.titan.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalReindexMapper extends Mapper<NullWritable, HadoopVertex, NullWritable, NullWritable> {

    private static final Logger log =
            LoggerFactory.getLogger(InternalReindexMapper.class);

    @Override
    public void map(final NullWritable key, final HadoopVertex value, final Mapper<NullWritable, HadoopVertex, NullWritable, NullWritable>.Context context) {
        log.info("HadoopVertex value: {}", value);
    }
}
