package com.thinkaurelius.titan.hadoop.formats.hbase.giraph;

import com.thinkaurelius.titan.hadoop.formats.util.GiraphVertexInputFormat;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

public class HBaseGiraphVertexInputFormat extends GiraphVertexInputFormat {

    public HBaseGiraphVertexInputFormat() {
        super(new HBaseGiraphInputFormat());
    }
}
