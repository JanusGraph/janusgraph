package com.thinkaurelius.titan.hadoop.formats.hbase;

import com.thinkaurelius.titan.hadoop.formats.util.GiraphInputFormat;

public class HBaseinputFormat extends GiraphInputFormat {
    public HBaseinputFormat() {
        super(new HBaseBinaryInputFormat());
    }
}
