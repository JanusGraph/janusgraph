package com.thinkaurelius.titan.hadoop.formats.hbase.giraph;

import com.thinkaurelius.titan.hadoop.formats.hbase.HBaseBinaryInputFormat;
import com.thinkaurelius.titan.hadoop.formats.util.GiraphInputFormat;

public class HBaseGiraphInputFormat extends GiraphInputFormat {
    public HBaseGiraphInputFormat() {
        super(new HBaseBinaryInputFormat());
    }
}
