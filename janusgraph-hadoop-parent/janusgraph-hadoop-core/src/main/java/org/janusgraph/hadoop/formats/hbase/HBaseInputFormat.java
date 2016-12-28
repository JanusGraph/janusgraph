package com.thinkaurelius.titan.hadoop.formats.hbase;

import com.thinkaurelius.titan.hadoop.formats.util.GiraphInputFormat;

public class HBaseInputFormat extends GiraphInputFormat {
    public HBaseInputFormat() {
        super(new HBaseBinaryInputFormat());
    }
}
