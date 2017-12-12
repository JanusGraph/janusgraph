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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Pair;
import org.janusgraph.HBaseStorageSetup;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.KeyColumnValueStoreUtil;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.junit.Assert;
import org.junit.Test;

public class HBaseStoreManagerMutationTest {

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
                    additions.add(e);
                } else {
                    // Collect the columns without TTL. Only do this for one row
                    if (row == 1) {
                        expectedColumnsWithoutTTL.add((long) i);
                    }
                    additions.add(e);
                }
            }
            // Add one deletion to the row
            if (row == 1) {
                expectedColumnDelete.add((long) (i - 1));
            }
            deletions.add(e);
            rowkeyMutationMap.put(rowkey, new KCVMutation(additions, deletions));
        }
        storeMutationMap.put("store1", rowkeyMutationMap);
        HBaseStoreManager manager = new HBaseStoreManager(HBaseStorageSetup.getHBaseConfiguration());
        final Map<StaticBuffer, Pair<List<Put>, Delete>> commandsPerRowKey
            = manager.convertToCommands(storeMutationMap, 0, 0);

        // 2 rows
        Assert.assertEquals(commandsPerRowKey.size(), 2);

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
        Assert.assertArrayEquals(expectedColumnsWithoutTTL.toArray(), putColumnsWithoutTTL.toArray());
        Assert.assertArrayEquals(expectedColumnsWithTTL.toArray(), putColumnsWithTTL.toArray());

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
        Assert.assertArrayEquals(expectedColumnDelete.toArray(), deleteColumns.toArray());
    }
}
