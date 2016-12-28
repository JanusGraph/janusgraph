package org.janusgraph.hadoop.formats.hbase;

import org.janusgraph.hadoop.formats.util.GiraphInputFormat;

public class HBaseInputFormat extends GiraphInputFormat {
    public HBaseInputFormat() {
        super(new HBaseBinaryInputFormat());
    }
}
