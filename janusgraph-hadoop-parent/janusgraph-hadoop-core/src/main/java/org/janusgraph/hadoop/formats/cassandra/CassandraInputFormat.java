package org.janusgraph.hadoop.formats.cassandra;

import org.janusgraph.hadoop.formats.util.GiraphInputFormat;

public class CassandraInputFormat extends GiraphInputFormat {
    public CassandraInputFormat() {
        super(new CassandraBinaryInputFormat());
    }
}
