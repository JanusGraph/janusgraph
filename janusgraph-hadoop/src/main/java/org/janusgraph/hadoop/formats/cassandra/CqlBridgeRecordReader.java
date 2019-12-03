/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.janusgraph.hadoop.formats.cassandra;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.TableMetadata;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * <p> Background: The {@link org.apache.cassandra.hadoop.cql3.CqlRecordReader} class has changed
 * significantly in Cassandra-3 from Cassandra-2. This class acts as a bridge between
 * CqlRecordReader in Cassandra-2 to Cassandra-3. In essence, this class recreates CqlRecordReader
 * from Cassandra-3 without referring to it (because otherwise we'd get functionality from
 * CqlRecordReader on Cassandra-2 and we don't want it). </p>
 *
 * @see <a href="https://github.com/JanusGraph/janusgraph/issues/172">Issue 172.</a>
 */
@Unstable // Because it should go away with JanusGraph upgrading to Cassandra-3 for OLAP
@Deprecated // Because this class should already be on its path to deprecation
public class CqlBridgeRecordReader extends RecordReader<StaticBuffer, Iterable<Entry>> {

    /* Implementation note: This is inspired by Cassandra-3's org/apache/cassandra/hadoop/cql3/CqlRecordReader.java */
    private static final Logger log = LoggerFactory.getLogger(CqlBridgeRecordReader.class);

    private ColumnFamilySplit split;
    private DistinctKeyIterator distinctKeyIterator;
    private int totalRowCount; // total number of rows to fetch
    private String keyspace;
    private String cfName;
    private String cqlQuery;
    private Cluster cluster;
    private Session session;
    private IPartitioner partitioner;
    private String inputColumns;
    private String userDefinedWhereClauses;

    private final List<String> partitionKeys = new ArrayList<>();

    // partition keys -- key aliases
    private final LinkedHashMap<String, Boolean> partitionBoundColumns = Maps.newLinkedHashMap();
    private int nativeProtocolVersion = 1;

    // binary type mapping code from CassandraBinaryRecordReader
    private KV currentKV;

    CqlBridgeRecordReader() { //package private
        super();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        this.split = (ColumnFamilySplit) split;
        Configuration conf = HadoopCompat.getConfiguration(context);
        totalRowCount = (this.split.getLength() < Long.MAX_VALUE)
            ? (int) this.split.getLength()
            : ConfigHelper.getInputSplitSize(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);
        keyspace = ConfigHelper.getInputKeyspace(conf);
        partitioner = ConfigHelper.getInputPartitioner(conf);
        inputColumns = CqlConfigHelper.getInputcolumns(conf);
        userDefinedWhereClauses = CqlConfigHelper.getInputWhereClauses(conf);

        try {
            if (cluster != null) {
                return;
            }
            // create a Cluster instance
            String[] locations = split.getLocations();
//            cluster = CqlConfigHelper.getInputCluster(locations, conf);
            // disregard the conf as it brings some unforeseen issues.
            cluster = Cluster.builder()
                .addContactPoints(locations)
                .build();
        } catch (Exception e) {
            throw new RuntimeException("Unable to create cluster for table: " + cfName + ", in keyspace: " + keyspace, e);
        }
        // cluster should be represent to a valid cluster now
        session = cluster.connect(quote(keyspace));
        Preconditions.checkNotNull(session, "Can't create connection session");
        //get negotiated serialization protocol
        nativeProtocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion().toInt();

        // If the user provides a CQL query then we will use it without validation
        // otherwise we will fall back to building a query using the:
        //   inputColumns
        //   whereClauses
        cqlQuery = CqlConfigHelper.getInputCql(conf);
        // validate that the user hasn't tried to give us a custom query along with input columns
        // and where clauses
        if (StringUtils.isNotEmpty(cqlQuery) && (StringUtils.isNotEmpty(inputColumns) ||
            StringUtils.isNotEmpty(userDefinedWhereClauses))) {
            throw new AssertionError("Cannot define a custom query with input columns and / or where clauses");
        }

        if (StringUtils.isEmpty(cqlQuery)) {
            cqlQuery = buildQuery();
        }
        log.trace("cqlQuery {}", cqlQuery);
        distinctKeyIterator = new DistinctKeyIterator();
        log.trace("created {}", distinctKeyIterator);
    }

    public void close() {
        if (session != null) {
            session.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    private static class KV {
        private final StaticArrayBuffer key;
        private ArrayList<Entry> entries;

        KV(StaticArrayBuffer key) {
            this.key = key;
        }

        void addEntries(Collection<Entry> toAdd) {
            if (entries == null) {
                entries = new ArrayList<>(toAdd.size());
            }
            entries.addAll(toAdd);
        }
    }

    @Override
    public StaticBuffer getCurrentKey() {
        return currentKV.key;
    }

    @Override
    public Iterable<Entry> getCurrentValue() throws IOException {
        return currentKV.entries;
    }

    public float getProgress() {
        if (!distinctKeyIterator.hasNext()) {
            return 1.0F;
        }

        // the progress is likely to be reported slightly off the actual but close enough
        float progress = ((float) distinctKeyIterator.totalRead / totalRowCount);
        return progress > 1.0F ? 1.0F : progress;
    }

    public boolean nextKeyValue() throws IOException {
        final Map<StaticArrayBuffer, Map<StaticBuffer, StaticBuffer>> kv = distinctKeyIterator.next();
        if (kv == null) {
            return false;
        }
        final Map.Entry<StaticArrayBuffer, Map<StaticBuffer, StaticBuffer>> onlyEntry = Iterables.getOnlyElement(kv.entrySet());
        final KV newKV = new KV(onlyEntry.getKey());
        final Map<StaticBuffer, StaticBuffer> v = onlyEntry.getValue();
        final List<Entry> entries = v.keySet()
                .stream()
                .map(column -> StaticArrayEntry.of(column, v.get(column)))
                .collect(toList());
        newKV.addEntries(entries);
        currentKV = newKV;
        return true;
    }

    /**
     * Return native version protocol of the cluster connection
     *
     * @return serialization protocol version.
     */
    public int getNativeProtocolVersion() {
        return nativeProtocolVersion;
    }

    /**
     * A non-static nested class that represents an iterator for distinct keys based on the
     * row iterator from DataStax driver. In the usual case, more than one row will be associated
     * with a single key in JanusGraph's use of Cassandra.
     */
    private class DistinctKeyIterator implements Iterator<Map<StaticArrayBuffer, Map<StaticBuffer, StaticBuffer>>> {
        public static final String KEY = "key";
        public static final String COLUMN_NAME = "column1";
        public static final String VALUE = "value";
        private final Iterator<Row> rowIterator;
        long totalRead;
        Row previousRow = null;
        DistinctKeyIterator() {
            AbstractType type = partitioner.getTokenValidator();
            Object startToken = type.compose(type.fromString(split.getStartToken()));
            Object endToken = type.compose(type.fromString(split.getEndToken()));
            SimpleStatement statement = new SimpleStatement(cqlQuery, startToken, endToken);
            rowIterator = session.execute(statement).iterator();
            for (ColumnMetadata meta : cluster.getMetadata().getKeyspace(quote(keyspace)).getTable(quote(cfName)).getPartitionKey()) {
                partitionBoundColumns.put(meta.getName(), Boolean.TRUE);
            }
        }

        @Override
        public boolean hasNext() {
            return rowIterator.hasNext();
        }

        /**
         * <p>
         *     Implements the <i>business logic</i> of the outer class. Refer to {@linkplain CqlBridgeRecordReader}.
         * Relies on the {@linkplain Iterator} of {@linkplain Row} to get a map of rows that correspond
         * to the same key.
         * </p>
         * <p>
         *     Note: This is not a general purpose iterator. There is no provision of {@linkplain java.util.ConcurrentModificationException}
         *     while iterating using this iterator.
         * </p>
         * @return the next element in the iteration of distinct keys, returns <code>null</code> to indicate
         * end of iteration
         */
        @Override
        public Map<StaticArrayBuffer, Map<StaticBuffer, StaticBuffer>> next() {
            if (! rowIterator.hasNext()) {
                return null; // null means no more data
            }
            Map<StaticArrayBuffer, Map<StaticBuffer, StaticBuffer>> keyColumnValues = new HashMap<>(); // key -> (column1 -> value)
            Row row;
            if (previousRow == null) {
                row = rowIterator.next(); // just the first time, should succeed
            } else {
                row = previousRow;
            }
            StaticArrayBuffer key = StaticArrayBuffer.of(row.getBytesUnsafe(KEY));
            StaticBuffer column1 = StaticArrayBuffer.of(row.getBytesUnsafe(COLUMN_NAME));
            StaticBuffer value = StaticArrayBuffer.of(row.getBytesUnsafe(VALUE));
            Map<StaticBuffer, StaticBuffer> cvs = new HashMap<>();
            cvs.put(column1, value);
            keyColumnValues.put(key, cvs);
            while (rowIterator.hasNext()) {
                Row nextRow = rowIterator.next();
                StaticArrayBuffer nextKey = StaticArrayBuffer.of(nextRow.getBytesUnsafe(KEY));
                if (! key.equals(nextKey)) {
                    previousRow = nextRow;
                    break;
                }
                StaticBuffer nextColumn = StaticArrayBuffer.of(nextRow.getBytesUnsafe(COLUMN_NAME));
                StaticBuffer nextValue = StaticArrayBuffer.of(nextRow.getBytesUnsafe(VALUE));
                cvs.put(nextColumn, nextValue);
                totalRead++;
            }
            return keyColumnValues;
        }
    }
    /**
     * Build a query for the reader of the form:
     *
     * SELECT * FROM ks>cf token(pk1,...pkn)>? AND token(pk1,...pkn)<=? [AND user where clauses]
     * [ALLOW FILTERING]
     */
    private String buildQuery() {
        fetchKeys();

        List<String> columns = getSelectColumns();
        String selectColumnList = columns.size() == 0 ? "*" : makeColumnList(columns);
        String partitionKeyList = makeColumnList(partitionKeys);

        return String.format("SELECT %s FROM %s.%s WHERE token(%s)>? AND token(%s)<=?" + getAdditionalWhereClauses(),
            selectColumnList, quote(keyspace), quote(cfName), partitionKeyList, partitionKeyList);
    }

    private String getAdditionalWhereClauses() {
        String whereClause = "";
        if (StringUtils.isNotEmpty(userDefinedWhereClauses)) {
            whereClause += " AND " + userDefinedWhereClauses;
        }
        if (StringUtils.isNotEmpty(userDefinedWhereClauses)) {
            whereClause += " ALLOW FILTERING";
        }
        return whereClause;
    }

    private List<String> getSelectColumns() {
        List<String> selectColumns = new ArrayList<>();

        if (StringUtils.isNotEmpty(inputColumns)) {
            // We must select all the partition keys plus any other columns the user wants
            selectColumns.addAll(partitionKeys);
            for (String column : Splitter.on(',').split(inputColumns)) {
                if (!partitionKeys.contains(column)) {
                    selectColumns.add(column);
                }
            }
        }
        return selectColumns;
    }

    private String makeColumnList(Collection<String> columns) {
        return columns.stream().map(this::quote).collect(Collectors.joining(","));
    }

    private void fetchKeys() {
        // get CF meta data
        TableMetadata tableMetadata = session.getCluster()
            .getMetadata()
            .getKeyspace(Metadata.quote(keyspace))
            .getTable(Metadata.quote(cfName));
        if (tableMetadata == null) {
            throw new RuntimeException("No table metadata found for " + keyspace + "." + cfName);
        }
        //Here we assume that tableMetadata.getPartitionKey() always
        //returns the list of columns in order of component_index
        for (ColumnMetadata partitionKey : tableMetadata.getPartitionKey()) {
            partitionKeys.add(partitionKey.getName());
        }
    }

    private String quote(String identifier) {
        return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
    }

}
