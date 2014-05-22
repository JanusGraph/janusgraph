package com.thinkaurelius.titan.diskstorage.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression;

public class HBaseCompat0_98 implements HBaseCompat {

    public void setCompression(HColumnDescriptor cd, String algo) {
        cd.setCompressionType(Compression.Algorithm.valueOf(algo));
    }
}
