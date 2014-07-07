package com.thinkaurelius.titan.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsReindexMapper extends Mapper<Object, Object, NullWritable, NullWritable> {

    private static final Logger log =
            LoggerFactory.getLogger(EsReindexMapper.class);

    @Override
    public void map(final Object key, final Object value, final Mapper<Object, Object, NullWritable, NullWritable>.Context context) {
        log.info("ES Mapping: {}={}", key, value);
    }
}
