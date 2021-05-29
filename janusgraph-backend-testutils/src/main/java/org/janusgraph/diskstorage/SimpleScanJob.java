// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage;


import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.Hex;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.util.system.ConfigurationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SimpleScanJob implements ScanJob {

    public static final String TOTAL_COUNT = "total";
    public static final String KEY_COUNT = "keys";
    public static final String SETUP_COUNT = "setup";
    public static final String TEARDOWN_COUNT = "teardown";

    public static final ConfigNamespace ROOT_NS = new ConfigNamespace(null, "simplescan", "testing job");

    public static final ConfigOption<String> HEX_QUERIES =
            new ConfigOption<>(ROOT_NS, "queries", "comma-delimited, hex-encoded queries",
                    ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<Long> KEY_FILTER_ID_MODULUS =
            new ConfigOption<>(ROOT_NS, "id-modulus",
                    "ID extracted from key must be divisible by this to pass the key filter",
                    ConfigOption.Type.LOCAL, Long.class);

    public static final ConfigOption<Long> KEY_FILTER_ID_MODULAR_VALUE =
            new ConfigOption<>(ROOT_NS, "id-modular-value",
                    "ID in modular arithmetic",
                    ConfigOption.Type.LOCAL, Long.class);

    private List<SliceQuery> qs;
    private Predicate<StaticBuffer> keyFilter;

    public SimpleScanJob() {
        qs = null;
        keyFilter = k -> true;
    }

    public SimpleScanJob(List<SliceQuery> qs, Predicate<StaticBuffer> keyFilter) {
        this.qs = qs;
        this.keyFilter = keyFilter;
    }

    @Override
    public SimpleScanJob clone() {
        return new SimpleScanJob(qs,keyFilter);
    }

    public SimpleScanJob(SliceQuery q) {
        this(ImmutableList.of(q),k -> true);
    }

    @Override
    public void workerIterationStart(Configuration config, Configuration graphConfig, ScanMetrics metrics) {
        assertNotNull(config);
        metrics.incrementCustom(SETUP_COUNT);

        if (config.has(HEX_QUERIES)) {
            String[] queryStrings = config.get(HEX_QUERIES).split(":");
            List<SliceQuery> queries = new LinkedList<>();
            for (String qString : queryStrings) {
                String[] queryTokens = qString.split("/");
                StaticBuffer start = StaticArrayBuffer.of(Hex.hexToBytes(queryTokens[0]));
                StaticBuffer end = StaticArrayBuffer.of(Hex.hexToBytes(queryTokens[1]));
                SliceQuery query = new SliceQuery(start, end);
                int limit = Integer.parseInt(queryTokens[2]);
                if (0 <= limit) {
                    query.setLimit(limit);
                }
                queries.add(query);
            }
            qs = queries;
        }

        if (config.has(KEY_FILTER_ID_MODULUS)) {
            final long mod = config.get(KEY_FILTER_ID_MODULUS);
            final long modVal;
            if (config.has(KEY_FILTER_ID_MODULAR_VALUE)) {
                modVal = config.get(KEY_FILTER_ID_MODULAR_VALUE);
            } else {
                modVal = 0;
            }
            keyFilter = k -> KeyValueStoreUtil.getID(k) % mod == modVal;
        }
    }

    public void workerIterationEnd(ScanMetrics metrics) {
        metrics.incrementCustom(TEARDOWN_COUNT);
    }


    @Override
    public void process(StaticBuffer key, Map<SliceQuery, EntryList> entries, ScanMetrics metrics) {
        assertNotNull(key);
        assertTrue(keyFilter.test(key));
        metrics.incrementCustom(KEY_COUNT);
        assertNotNull(entries);
        assertTrue(qs.size() >= entries.size());
        for (SliceQuery q : qs) {
            if (!entries.containsKey(q)) {
                continue;
            }
            EntryList result = entries.get(q);
            metrics.incrementCustom(TOTAL_COUNT,result.size());
        }
    }

    @Override
    public List<SliceQuery> getQueries() {
        return qs;
    }

    @Override
    public Predicate<StaticBuffer> getKeyFilter() {
        return keyFilter;
    }

    private static String encodeQueries(List<SliceQuery> queries) {

        List<String> queryStrings = new ArrayList<>(queries.size());

        for (SliceQuery query : queries) {
            String start = Hex.bytesToHex(query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY));
            String end = Hex.bytesToHex(query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY));

            final int limit;
            if (query.hasLimit()) {
                limit = query.getLimit();
            } else {
                limit = -1;
            }

            queryStrings.add(String.format("%s/%s/%d", start, end, limit));
        }

        return Joiner.on(":").join(queryStrings);
    }

    public static void runBasicTests(int keys, int columns, SimpleScanJobRunner runner)
            throws InterruptedException, ExecutionException, BackendException, IOException {

        Configuration conf1 = getJobConf(
                ImmutableList.of(new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128))));
        ScanMetrics result1 = runner.run(new SimpleScanJob(), conf1, SimpleScanJob.class.getName() + "#ROOT_NS");
        assertEquals(keys,result1.getCustom(SimpleScanJob.KEY_COUNT));
        assertEquals(keys*columns/4*3,result1.getCustom(SimpleScanJob.TOTAL_COUNT));
        /* These assertions are not valid on Hadoop.  The Hadoop implementation uses
         * Hadoop Counters to store ScanMetrics.  These Counters are shared
         * clusterwide.  Hence there will be as many setups and teardowns as there
         * are input splits -- generally more than one.  So these don't apply:
         *
         *  assertEquals(1, result1.getCustom(SimpleScanJob.SETUP_COUNT));
         *  assertEquals(1, result1.getCustom(SimpleScanJob.TEARDOWN_COUNT));
         *
         * However, even on Hadoop, we can expect both of the following to hold:
         * 1. The number of setups must equal the number of teardowns
         * 2. The number of setups (teardowns) must be positive
         */
        assertEquals(result1.getCustom(SimpleScanJob.SETUP_COUNT), result1.getCustom(SimpleScanJob.TEARDOWN_COUNT),
            "Number of ScanJob setup calls must equal number of ScanJob teardown calls");
        assertTrue(0 < result1.getCustom(SimpleScanJob.SETUP_COUNT),
            "Number of ScanJob setup/teardown calls must be positive");

        Configuration conf2 = getJobConf(ImmutableList.of(new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128)).setLimit(5)));
        ScanMetrics result2 = runner.run(new SimpleScanJob(), conf2, SimpleScanJob.class.getName() + "#ROOT_NS");
        assertEquals(keys,result2.getCustom(SimpleScanJob.KEY_COUNT));
        assertEquals(keys*5,result2.getCustom(SimpleScanJob.TOTAL_COUNT));

        Configuration conf3 = getJobConf(ImmutableList.of(new SliceQuery(KeyValueStoreUtil.getBuffer(0), KeyValueStoreUtil.getBuffer(5))));
        ScanMetrics result3 = runner.run(new SimpleScanJob(), conf3, SimpleScanJob.class.getName() + "#ROOT_NS");
        assertEquals(keys,result3.getCustom(SimpleScanJob.KEY_COUNT));
        assertEquals(keys*5,result3.getCustom(SimpleScanJob.TOTAL_COUNT));

        Configuration conf4 = getJobConf(ImmutableList.of(
                new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128)).setLimit(1),
                new SliceQuery(KeyValueStoreUtil.getBuffer(0), KeyValueStoreUtil.getBuffer(5))));
        ScanMetrics result4 = runner.run(new SimpleScanJob(), conf4, SimpleScanJob.class.getName() + "#ROOT_NS");
        assertEquals(keys,result4.getCustom(SimpleScanJob.KEY_COUNT));
        assertEquals(keys*6,result4.getCustom(SimpleScanJob.TOTAL_COUNT));

        Configuration conf5 = getJobConf(ImmutableList.of(
                new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128)).setLimit(1),
                new SliceQuery(KeyValueStoreUtil.getBuffer(2), KeyValueStoreUtil.getBuffer(4)),
                new SliceQuery(KeyValueStoreUtil.getBuffer(6), KeyValueStoreUtil.getBuffer(8)),
                new SliceQuery(KeyValueStoreUtil.getBuffer(10), KeyValueStoreUtil.getBuffer(20)).setLimit(4)));

        ScanMetrics result5 = runner.run(new SimpleScanJob(), conf5, SimpleScanJob.class.getName() + "#ROOT_NS");
        assertEquals(keys, result5.getCustom(SimpleScanJob.KEY_COUNT));
        assertEquals(keys*9,result5.getCustom(SimpleScanJob.TOTAL_COUNT));

        Configuration conf6 = getJobConf(
                ImmutableList.of(new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128)).setLimit(5)), 2L);
        ScanMetrics result6 = runner.run(new SimpleScanJob(), conf6, SimpleScanJob.class.getName() + "#ROOT_NS");
        assertEquals(keys/2,result6.getCustom(SimpleScanJob.KEY_COUNT));
        assertEquals(keys/2*5,result6.getCustom(SimpleScanJob.TOTAL_COUNT));


        Configuration conf7 = getJobConf(ImmutableList.of(
                new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128)).setLimit(1),
                new SliceQuery(KeyValueStoreUtil.getBuffer(2), KeyValueStoreUtil.getBuffer(4)),
                new SliceQuery(KeyValueStoreUtil.getBuffer(31), KeyValueStoreUtil.getBuffer(35)),
                new SliceQuery(KeyValueStoreUtil.getBuffer(36), KeyValueStoreUtil.getBuffer(40)).setLimit(1)));
        ScanMetrics result7 = runner.run(new SimpleScanJob(), conf7, SimpleScanJob.class.getName() + "#ROOT_NS");
        assertEquals(keys,result7.getCustom(SimpleScanJob.KEY_COUNT));
        assertEquals(keys*3+keys/2*5,result7.getCustom(SimpleScanJob.TOTAL_COUNT));

        Configuration conf8 = getJobConf(ImmutableList.of(
                new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128)).setLimit(1),
                new SliceQuery(KeyValueStoreUtil.getBuffer(31), KeyValueStoreUtil.getBuffer(35))), 2L, 1L);
        ScanMetrics result8 = runner.run(new SimpleScanJob(), conf8, SimpleScanJob.class.getName() + "#ROOT_NS");
//                        k -> KeyValueStoreUtil.getID(k) % 2 == 1));
        assertEquals(keys/2,result8.getCustom(SimpleScanJob.KEY_COUNT));
        assertEquals(keys/2*5,result8.getCustom(SimpleScanJob.TOTAL_COUNT));

        Configuration conf9 = getJobConf(ImmutableList.of(
                    new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128)).setLimit(1),
                    new SliceQuery(KeyValueStoreUtil.getBuffer(31), KeyValueStoreUtil.getBuffer(35))), 2L);
//                        k -> KeyValueStoreUtil.getID(k) % 2 == 0));
        ScanMetrics result9 = runner.run(new SimpleScanJob(), conf9, SimpleScanJob.class.getName() + "#ROOT_NS");
        assertEquals(keys/2,result9.getCustom(SimpleScanJob.KEY_COUNT));
        assertEquals(keys/2,result9.getCustom(SimpleScanJob.TOTAL_COUNT));

        try {
            Configuration conf10 = getJobConf(ImmutableList.of(
                    new SliceQuery(StaticArrayBuffer.of(new byte[]{(byte) 2}), BufferUtil.oneBuffer(1)),
                    new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1))));
            runner.run(new SimpleScanJob(), conf10, SimpleScanJob.class.getName() + "#ROOT_NS");
            fail();
        } catch (Exception e) {
            //assertTrue(e instanceof ExecutionException && e.getCause() instanceof IllegalArgumentException);
        }
    }

    public static Configuration getJobConf(List<SliceQuery> queries) {
        return getJobConf(queries, null, null);
    }

    public static Configuration getJobConf(List<SliceQuery> queries, Long modulus) {
        return getJobConf(queries, modulus, null);
    }

    public static Configuration getJobConf(List<SliceQuery> queries, Long modulus, Long modVal) {
        ModifiableConfiguration conf2 =
                new ModifiableConfiguration(SimpleScanJob.ROOT_NS,
                        new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration()), BasicConfiguration.Restriction.NONE);
        if (null != queries)
            conf2.set(HEX_QUERIES, encodeQueries(queries));
        if (null != modulus)
            conf2.set(KEY_FILTER_ID_MODULUS, modulus);
        if (null != modVal)
            conf2.set(KEY_FILTER_ID_MODULAR_VALUE, modVal);
        return conf2;
    }
}
