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

package org.janusgraph.diskstorage.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Pair;
import org.janusgraph.HBaseContainer;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.KeyColumnValueStoreUtil;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class HBaseStoreManagerMutationTest {
    @Container
    public static final HBaseContainer hBaseContainer = new HBaseContainer();

    @Test
    public void testKCVMutationToPuts() throws Exception {
        final Map<String, Map<StaticBuffer, KCVMutation>> storeMutationMap = new HashMap<>();
        final Map<StaticBuffer, KCVMutation> rowkeyMutationMap = new HashMap<>();
        final List<Long> expectedColumnsWithTTL = new ArrayList<>();
        final List<Long> expectedColumnsWithoutTTL = new ArrayList<>();
        final List<Long> expectedColumnDelete = new ArrayList<>();
        StaticArrayEntry e = null;
        StaticBuffer rowkey, col, val;
        // 2 rows
        for (int row = 0; row < 2; row++) {

            rowkey = KeyColumnValueStoreUtil.longToByteBuffer(row);

            List<Entry> additions = new ArrayList<>();
            List<StaticBuffer> deletions = new ArrayList<>();

            // 100 columns each row
            int i;
            for (i = 0; i < 100; i++) {
                col = KeyColumnValueStoreUtil.longToByteBuffer(i);
                val = KeyColumnValueStoreUtil.longToByteBuffer(i + 100);
                e = (StaticArrayEntry) StaticArrayEntry.of(col, val);
                // Set half of the columns with TTL, also vary the TTL values
                if (i % 2 == 0) {
                    e.setMetaData(EntryMetaData.TTL, i % 10 + 1);
                    // Collect the columns with TTL. Only do this for one row
                    if (row == 1) {
                      expectedColumnsWithTTL.add((long) i);
                    }
                } else {
                    // Collect the columns without TTL. Only do this for one row
                    if (row == 1) {
                        expectedColumnsWithoutTTL.add((long) i);
                    }
                }
                additions.add(e);
            }
            // Add one deletion to the row
            if (row == 1) {
                expectedColumnDelete.add((long) (i - 1));
            }
            deletions.add(e);
            rowkeyMutationMap.put(rowkey, new KCVMutation(additions, deletions));
        }
        storeMutationMap.put("store1", rowkeyMutationMap);
        HBaseStoreManager manager = new HBaseStoreManager(hBaseContainer.getModifiableConfiguration());
        final Map<StaticBuffer, Pair<List<Put>, Delete>> commandsPerRowKey
            = manager.convertToCommands(storeMutationMap, 0L, 0L);

        // 2 rows
        assertEquals(commandsPerRowKey.size(), 2);

        // Verify puts
        final List<Long> putColumnsWithTTL = new ArrayList<>();
        final List<Long> putColumnsWithoutTTL = new ArrayList<>();
        Pair<List<Put>, Delete> commands = commandsPerRowKey.values().iterator().next();
        long colName;
        for (Put p : commands.getFirst()) {
            // In Put, Long.MAX_VALUE means no TTL
            for (Map.Entry<byte[], List<Cell>> me : p.getFamilyCellMap().entrySet()) {
                for (Cell c : me.getValue()) {
                    colName = KeyColumnValueStoreUtil.bufferToLong(new StaticArrayBuffer(CellUtil.cloneQualifier(c)));
                    if (p.getTTL() < Long.MAX_VALUE) {
                        putColumnsWithTTL.add(colName);
                    } else {
                        putColumnsWithoutTTL.add(colName);
                    }
                }
            }
        }
        Collections.sort(putColumnsWithoutTTL);
        Collections.sort(putColumnsWithTTL);
        assertArrayEquals(expectedColumnsWithoutTTL.toArray(), putColumnsWithoutTTL.toArray());
        assertArrayEquals(expectedColumnsWithTTL.toArray(), putColumnsWithTTL.toArray());

        // Verify deletes
        final List<Long> deleteColumns = new ArrayList<>();
        Delete d = commands.getSecond();
        for (Map.Entry<byte[], List<Cell>> me : d.getFamilyCellMap().entrySet()) {
            for (Cell c : me.getValue()) {
                colName = KeyColumnValueStoreUtil.bufferToLong(new StaticArrayBuffer(CellUtil.cloneQualifier(c)));
                deleteColumns.add(colName);
            }
        }
        Collections.sort(deleteColumns);
        assertArrayEquals(expectedColumnDelete.toArray(), deleteColumns.toArray());
    }

    @Test
    public void testMutationToPutsTTL() throws Exception{
        final Map<String, Map<StaticBuffer, KCVMutation>> storeMutationMap = new HashMap<>();
        final Map<StaticBuffer, KCVMutation> rowkeyMutationMap = new HashMap<>();
        final List<Long> expectedColumnsWithTTL = new ArrayList<>();
        final List<Long> putColumnsWithTTL = new ArrayList<>();
        List<Entry> additions = new ArrayList<>();
        List<StaticBuffer> deletions = new ArrayList<>();

        StaticBuffer rowkey = KeyColumnValueStoreUtil.longToByteBuffer(0);
        StaticBuffer col = KeyColumnValueStoreUtil.longToByteBuffer(1);
        StaticBuffer val = KeyColumnValueStoreUtil.longToByteBuffer(2);

        StaticArrayEntry e = (StaticArrayEntry) StaticArrayEntry.of(col, val);
        //Test TTL with int max value / 1000 + 1
        //When convert this value from second to millisec will over Integer limit
        e.setMetaData(EntryMetaData.TTL, Integer.MAX_VALUE/1000+1);

        Integer ttl = (Integer) e.getMetaData().get(EntryMetaData.TTL);
        expectedColumnsWithTTL.add(TimeUnit.SECONDS.toMillis((long)ttl));//convert second to millisec with long format

        additions.add(e);
        deletions.add(e);
        rowkeyMutationMap.put(rowkey, new KCVMutation(additions, deletions));
        storeMutationMap.put("store1", rowkeyMutationMap);
        HBaseStoreManager manager = new HBaseStoreManager(hBaseContainer.getModifiableConfiguration());
        final Map<StaticBuffer, Pair<List<Put>, Delete>> commandsPerRowKey
            = manager.convertToCommands(storeMutationMap, 0L, 0L);
        Pair<List<Put>, Delete> commands = commandsPerRowKey.values().iterator().next();

        //Verify Put TTL
        Put put = commands.getFirst().get(0);
        putColumnsWithTTL.add(put.getTTL());
        assertArrayEquals(expectedColumnsWithTTL.toArray(), putColumnsWithTTL.toArray());
    }

    /**
     * Verifies that a {@link KCVMutation} with {@code setWholeRowDeletion(true)} and no per-column
     * deletions produces a column-family-level {@link Delete} (not per-qualifier deletes) in
     * {@link HBaseStoreManager#convertToCommands}.
     *
     * <p>Before the fix, a flag-only mutation with empty deletions list produced no Delete at all
     * (the old guard was {@code if (mutation.hasDeletions())}). After the fix it must produce a
     * Delete whose cell map contains exactly one {@link Cell.Type#DeleteFamily} cell, with no
     * qualifier bytes (i.e. qualifier length == 0) — the hallmark of a family-level tombstone.
     */
    @Test
    public void convertToCommandsWholeRowDeletionUsesFamilyDelete() throws Exception {
        final String storeName = "store1";
        final StaticBuffer rowkey = KeyColumnValueStoreUtil.longToByteBuffer(42);

        KCVMutation mutation = new KCVMutation(Collections.emptyList(), Collections.emptyList());
        mutation.setWholeRowDeletion(true);

        final Map<StaticBuffer, KCVMutation> rowkeyMutationMap = new HashMap<>();
        rowkeyMutationMap.put(rowkey, mutation);

        final Map<String, Map<StaticBuffer, KCVMutation>> storeMutationMap = new HashMap<>();
        storeMutationMap.put(storeName, rowkeyMutationMap);

        HBaseStoreManager manager = new HBaseStoreManager(hBaseContainer.getModifiableConfiguration());
        // Use a non-null delTimestamp so the timestamp branch is exercised.
        final long delTimestamp = System.currentTimeMillis();
        final Map<StaticBuffer, Pair<List<Put>, Delete>> commandsPerRowKey =
                manager.convertToCommands(storeMutationMap, null, delTimestamp);

        assertEquals(1, commandsPerRowKey.size(), "Expected exactly one rowkey entry");
        Pair<List<Put>, Delete> commands = commandsPerRowKey.values().iterator().next();

        // A whole-row deletion must produce a Delete (old code would have produced null).
        Delete d = commands.getSecond();
        assertNotNull(d, "Delete must be non-null for a whole-row-deletion mutation");

        // The Delete's family-cell map must contain at least one entry.
        assertNotNull(d.getFamilyCellMap(), "Family cell map must not be null");
        assertFalse(d.getFamilyCellMap().isEmpty(), "Family cell map must not be empty");

        // Every cell in the Delete must be a DeleteFamily tombstone (no per-qualifier delete).
        boolean hasDeleteFamily = false;
        for (Map.Entry<byte[], List<Cell>> familyEntry : d.getFamilyCellMap().entrySet()) {
            for (Cell c : familyEntry.getValue()) {
                Cell.Type type = c.getType();
                assertEquals(Cell.Type.DeleteFamily, type,
                        "Expected DeleteFamily cell type, got " + type + " — a per-column qualifier delete was produced instead of a family delete");
                // A family delete cell has a zero-length qualifier.
                assertEquals(0, c.getQualifierLength(),
                        "Family delete cell must have an empty qualifier");
                hasDeleteFamily = true;
            }
        }
        assertTrue(hasDeleteFamily, "At least one DeleteFamily cell must be present");

        // No puts should have been produced (the mutation had no additions).
        assertTrue(commands.getFirst().isEmpty(), "Expected no Put commands for a whole-row-deletion-only mutation");
    }
}
