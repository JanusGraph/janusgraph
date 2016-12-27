package com.thinkaurelius.titan.hadoop.formats.cassandra;

import com.thinkaurelius.titan.hadoop.formats.util.GiraphInputFormat;

public class CassandraInputFormat extends GiraphInputFormat {
    public CassandraInputFormat() {
        super(new CassandraBinaryInputFormat());
    }
}
