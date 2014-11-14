package com.thinkaurelius.titan.hadoop.formats.cassandra.giraph;

import com.thinkaurelius.titan.hadoop.formats.util.GiraphVertexInputFormat;

public class CassandraGiraphVertexInputFormat extends GiraphVertexInputFormat {

    public CassandraGiraphVertexInputFormat() {
        super(new CassandraGiraphInputFormat());
    }
}