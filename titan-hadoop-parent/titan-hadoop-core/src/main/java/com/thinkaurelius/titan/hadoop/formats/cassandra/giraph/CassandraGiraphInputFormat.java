package com.thinkaurelius.titan.hadoop.formats.cassandra.giraph;

import com.thinkaurelius.titan.hadoop.formats.cassandra.CassandraBinaryInputFormat;
import com.thinkaurelius.titan.hadoop.formats.util.GiraphInputFormat;

public class CassandraGiraphInputFormat extends GiraphInputFormat {

    public CassandraGiraphInputFormat() {
        super(new CassandraBinaryInputFormat());
    }
}
