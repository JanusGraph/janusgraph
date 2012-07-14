package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface MapStep {
    
    public FaunusVertex doMap(FaunusVertex vertex, Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context);
}
