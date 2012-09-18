package com.thinkaurelius.faunus.formats.titan.cassandra;
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


import com.google.common.collect.ImmutableList;
import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Adoped from Cassandra's source code base.
 * <p/>
 * Hadoop InputFormat allowing map/reduce against Cassandra rows within one ColumnFamily.
 * <p/>
 * At minimum, you need to set the CF and predicate (description of columns to extract from each row)
 * in your Hadoop job Configuration.  The ConfigHelper class is provided to make this
 * simple:
 * ConfigHelper.setColumnFamily
 * ConfigHelper.setSlicePredicate
 * <p/>
 * You can also configure the number of rows per InputSplit with
 * ConfigHelper.setInputSplitSize
 * This should be "as big as possible, but no bigger."  Each InputSplit is read from Cassandra
 * with multiple get_slice_range queries, and the per-call overhead of get_slice_range is high,
 * so larger split sizes are better -- but if it is too large, you will run out of memory.
 * <p/>
 * The default split size is 64k rows.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraInputFormat extends InputFormat<NullWritable, FaunusVertex> {

    private static final Logger logger = LoggerFactory.getLogger(TitanCassandraInputFormat.class);

    private String keyspace;
    private String cfName;
    private IPartitioner partitioner;

    private FaunusTitanCassandraGraph graph = null;

    public RecordReader<NullWritable, FaunusVertex> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        if (graph == null) {
            final Configuration conf = taskAttemptContext.getConfiguration();
            //  ## Instantiate Titan ##
            final BaseConfiguration titanconfig = new BaseConfiguration();
            //General Titan configuration for read-only
            titanconfig.setProperty("storage.read-only", "true");
            titanconfig.setProperty("autotype", "none");
            //Cassandra specific configuration
            titanconfig.setProperty("storage.backend", "cassandra");   // todo: astyanax
            titanconfig.setProperty("storage.hostname", ConfigHelper.getInputInitialAddress(conf));
            titanconfig.setProperty("storage.keyspace", ConfigHelper.getInputKeyspace(conf));
            titanconfig.setProperty("storage.port", ConfigHelper.getInputRpcPort(conf));
            if (ConfigHelper.getReadConsistencyLevel(conf) != null)
                titanconfig.setProperty("storage.read-consistency-level", ConfigHelper.getReadConsistencyLevel(conf));
            if (ConfigHelper.getWriteConsistencyLevel(conf) != null)
                titanconfig.setProperty("storage.write-consistency-level", ConfigHelper.getWriteConsistencyLevel(conf));
            graph = new FaunusTitanCassandraGraph(titanconfig);
        }
        return new TitanCassandraRecordReader(graph);
    }

    @Override
    public void finalize() throws Throwable {
        super.finalize();
        if (graph != null) graph.shutdown();
    }

    public List<InputSplit> getSplits(final JobContext context) throws IOException {
        final Configuration conf = context.getConfiguration();
        validateConfiguration(conf);

        // cannonical ranges and nodes holding replicas
        List<TokenRange> masterRangeNodes = getRangeMap(conf);

        keyspace = ConfigHelper.getInputKeyspace(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);
        partitioner = ConfigHelper.getInputPartitioner(conf);
        logger.debug("partitioner is " + partitioner);

        // cannonical ranges, split into pieces, fetching the splits in parallel
        ExecutorService executor = Executors.newCachedThreadPool();
        List<InputSplit> splits = new ArrayList<InputSplit>();

        try {
            final List<Future<List<InputSplit>>> splitfutures = new ArrayList<Future<List<InputSplit>>>();
            final KeyRange jobKeyRange = ConfigHelper.getInputKeyRange(conf);
            Range<Token> jobRange = null;
            if (jobKeyRange != null && jobKeyRange.start_token != null) {
                assert partitioner.preservesOrder() : "ConfigHelper.setInputKeyRange(..) can only be used with an order preserving paritioner";
                assert jobKeyRange.start_key == null : "Only start_token supported";
                assert jobKeyRange.end_key == null : "Only end_token supported";
                jobRange = new Range<Token>(partitioner.getTokenFactory().fromString(jobKeyRange.start_token),
                        partitioner.getTokenFactory().fromString(jobKeyRange.end_token),
                        partitioner);
            }

            for (final TokenRange range : masterRangeNodes) {
                if (jobRange == null) {
                    // for each range, pick a live owner and ask it to compute bite-sized splits
                    splitfutures.add(executor.submit(new SplitCallable(range, conf)));
                } else {
                    Range<Token> dhtRange = new Range<Token>(partitioner.getTokenFactory().fromString(range.start_token),
                            partitioner.getTokenFactory().fromString(range.end_token),
                            partitioner);

                    if (dhtRange.intersects(jobRange)) {
                        for (Range<Token> intersection : dhtRange.intersectionWith(jobRange)) {
                            range.start_token = partitioner.getTokenFactory().toString(intersection.left);
                            range.end_token = partitioner.getTokenFactory().toString(intersection.right);
                            // for each range, pick a live owner and ask it to compute bite-sized splits
                            splitfutures.add(executor.submit(new SplitCallable(range, conf)));
                        }
                    }
                }
            }

            // wait until we have all the results back
            for (final Future<List<InputSplit>> futureInputSplits : splitfutures) {
                try {
                    splits.addAll(futureInputSplits.get());
                } catch (Exception e) {
                    throw new IOException("Could not retrieve input splits", e);
                }
            }
        } finally {
            executor.shutdownNow();
        }

        assert splits.size() > 0;
        Collections.shuffle(splits, new Random(System.nanoTime()));
        return splits;
    }

    /**
     * Gets a token range and splits it up according to the suggested
     * size into input splits that Hadoop can use.
     */
    class SplitCallable implements Callable<List<InputSplit>> {

        private final TokenRange range;
        private final Configuration conf;

        public SplitCallable(TokenRange tr, Configuration conf) {
            this.range = tr;
            this.conf = conf;
        }

        public List<InputSplit> call() throws Exception {
            final List<InputSplit> splits = new ArrayList<InputSplit>();
            final List<String> tokens = getSubSplits(keyspace, cfName, range, conf);
            assert range.rpc_endpoints.size() == range.endpoints.size() : "rpc_endpoints size must match endpoints size";
            // turn the sub-ranges into InputSplits
            final String[] endpoints = range.endpoints.toArray(new String[range.endpoints.size()]);
            // hadoop needs hostname, not ip
            int endpointIndex = 0;
            for (final String endpoint : range.rpc_endpoints) {
                String endpoint_address = endpoint;
                if (endpoint_address == null || endpoint_address.equals("0.0.0.0"))
                    endpoint_address = range.endpoints.get(endpointIndex);
                endpoints[endpointIndex++] = InetAddress.getByName(endpoint_address).getHostName();
            }

            final Token.TokenFactory factory = partitioner.getTokenFactory();
            for (int i = 1; i < tokens.size(); i++) {
                final Token left = factory.fromString(tokens.get(i - 1));
                final Token right = factory.fromString(tokens.get(i));
                final Range<Token> range = new Range<Token>(left, right, partitioner);
                final List<Range<Token>> ranges = range.isWrapAround() ? range.unwrap() : ImmutableList.of(range);
                for (final Range<Token> subrange : ranges) {
                    ColumnFamilySplit split = new ColumnFamilySplit(factory.toString(subrange.left), factory.toString(subrange.right), endpoints);
                    logger.debug("Adding " + split);
                    splits.add(split);
                }
            }
            return splits;
        }
    }

    private List<String> getSubSplits(String keyspace, String cfName, TokenRange range, Configuration conf) throws IOException {
        int splitsize = ConfigHelper.getInputSplitSize(conf);
        for (int i = 0; i < range.rpc_endpoints.size(); i++) {
            String host = range.rpc_endpoints.get(i);

            if (host == null || host.equals("0.0.0.0"))
                host = range.endpoints.get(i);

            try {
                Cassandra.Client client = ConfigHelper.createConnection(host, ConfigHelper.getInputRpcPort(conf), true);
                client.set_keyspace(keyspace);
                return client.describe_splits(cfName, range.start_token, range.end_token, splitsize);
            } catch (IOException e) {
                logger.debug("Failed to connect to endpoint " + host, e);
            } catch (TException e) {
                throw new RuntimeException(e);
            } catch (InvalidRequestException e) {
                throw new RuntimeException(e);
            }
        }
        throw new IOException("Failed to connect to all endpoints " + StringUtils.join(range.endpoints, ","));
    }


    private List<TokenRange> getRangeMap(final Configuration conf) throws IOException {
        final Cassandra.Client client = ConfigHelper.getClientFromInputAddressList(conf);

        final List<TokenRange> map;
        try {
            map = client.describe_ring(ConfigHelper.getInputKeyspace(conf));
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        }
        return map;
    }


    private static void validateConfiguration(final Configuration conf) {
        if (ConfigHelper.getInputKeyspace(conf) == null)
            throw new UnsupportedOperationException("The keyspace configuration must be set");

        if (ConfigHelper.getInputColumnFamily(conf) == null)
            throw new UnsupportedOperationException("The columnfamily configuration must be set (with setColumnFamily())");

        if (ConfigHelper.getInputSlicePredicate(conf) == null)
            throw new UnsupportedOperationException("The predicate configuration must be set (with setPredicate())");

        if (ConfigHelper.getInputInitialAddress(conf) == null)
            throw new UnsupportedOperationException("The initial input address configuration must be set");

        if (ConfigHelper.getInputPartitioner(conf) == null)
            throw new UnsupportedOperationException("The Cassandra partitioner class configuration must be set");
    }

}