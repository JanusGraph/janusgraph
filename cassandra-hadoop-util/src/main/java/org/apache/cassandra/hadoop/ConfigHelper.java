/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.hadoop;

import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;

public class ConfigHelper
{
    private static final String INPUT_PARTITIONER_CONFIG = "cassandra.input.partitioner.class";
    private static final String INPUT_KEYSPACE_CONFIG = "cassandra.input.keyspace";
    private static final String INPUT_COLUMNFAMILY_CONFIG = "cassandra.input.columnfamily";
    private static final String INPUT_KEYRANGE_CONFIG = "cassandra.input.keyRange";
    private static final String INPUT_SPLIT_SIZE_CONFIG = "cassandra.input.split.size";
    private static final String INPUT_SPLIT_SIZE_IN_MB_CONFIG = "cassandra.input.split.size_mb";
    private static final String INPUT_WIDEROWS_CONFIG = "cassandra.input.widerows";
    private static final int DEFAULT_SPLIT_SIZE = 64 * 1024;
    private static final String INPUT_INITIAL_ADDRESS = "cassandra.input.address";
    private static final String READ_CONSISTENCY_LEVEL = "cassandra.consistencylevel.read";

    /**
     * Set the keyspace and column family for the input of this job.
     *
     * @param conf         Job configuration you are about to run
     * @param keyspace
     * @param columnFamily
     * @param widerows
     */
    public static void setInputColumnFamily(Configuration conf, String keyspace, String columnFamily, boolean widerows)
    {
        if (keyspace == null)
            throw new UnsupportedOperationException("keyspace may not be null");

        if (columnFamily == null)
            throw new UnsupportedOperationException("table may not be null");

        conf.set(INPUT_KEYSPACE_CONFIG, keyspace);
        conf.set(INPUT_COLUMNFAMILY_CONFIG, columnFamily);
        conf.set(INPUT_WIDEROWS_CONFIG, String.valueOf(widerows));
    }

    public static int getInputSplitSize(Configuration conf)
    {
        return conf.getInt(INPUT_SPLIT_SIZE_CONFIG, DEFAULT_SPLIT_SIZE);
    }


    /**
     * cassandra.input.split.size will be used if the value is undefined or negative.
     * @param conf  Job configuration you are about to run
     * @return      split size in MB or -1 if it is undefined.
     */
    public static int getInputSplitSizeInMb(Configuration conf)
    {
        return conf.getInt(INPUT_SPLIT_SIZE_IN_MB_CONFIG, -1);
    }


    /**
     * The start and end token of the input key range as a pair.
     *
     * may be null if unset.
     */
    public static Pair<String, String> getInputKeyRange(Configuration conf)
    {
        String str = conf.get(INPUT_KEYRANGE_CONFIG);
        if (str == null)
            return null;

        String[] parts = str.split(",");
        assert parts.length == 2;
        return Pair.create(parts[0], parts[1]);
    }

    public static String getInputKeyspace(Configuration conf)
    {
        return conf.get(INPUT_KEYSPACE_CONFIG);
    }

    public static String getInputColumnFamily(Configuration conf)
    {
        return conf.get(INPUT_COLUMNFAMILY_CONFIG);
    }


    public static String getReadConsistencyLevel(Configuration conf)
    {
        return conf.get(READ_CONSISTENCY_LEVEL, "LOCAL_ONE");
    }

    public static String getInputInitialAddress(Configuration conf)
    {
        return conf.get(INPUT_INITIAL_ADDRESS);
    }

    public static void setInputInitialAddress(Configuration conf, String address)
    {
        conf.set(INPUT_INITIAL_ADDRESS, address);
    }

    public static String getInputPartitioner(Configuration conf)
    {
        return conf.get(INPUT_PARTITIONER_CONFIG);
    }
}
