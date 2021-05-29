// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.diskstorage.inmemory;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.janusgraph.diskstorage.inmemory.BufferPageUtils.buildFromEntryArray;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BufferPageTest
{
    private static final String COL_START = "00colStart";
    private static final String COL_END = "99colEnd";
    private static final String VERY_END = "zz";

    static StaticArrayBuffer makeStaticBuffer(String value) {
        return StaticArrayBuffer.of(value.getBytes());
    }

    static Entry makeEntry(String column, String value) {
        return StaticArrayEntry.of(makeStaticBuffer(column), makeStaticBuffer(value));
    }

    @Test
    public void testGetIndex()
    {
        Entry[] entries = new Entry[]
                {
                    makeEntry(COL_START, "valStart"),
                    makeEntry("01col1", "val1"),
                    makeEntry("02col2", "val2"),
                    makeEntry("03col3", "val3"),
                    makeEntry("04emptycol", ""),
                    makeEntry(COL_END, "valEnd"),
                }; //must be pre-sorted

        BufferPage bp = buildFromEntryArray(entries, entries.length);

        for(int i=0; i<entries.length; i++)
        {
            assertEquals(i, bp.getIndex(entries[i].getColumn()));
        }

        assertEquals(-1, bp.getIndex(StaticArrayBuffer.of(COL_START.substring(0, 1).getBytes())));
        assertEquals(-entries.length - 1, bp.getIndex(StaticArrayBuffer.of(VERY_END.getBytes())));
    }

    @Test
    public void testMerge()
    {
        Entry[] entries = new Entry[]
                {
                        makeEntry(COL_START, "valStart"),
                        makeEntry("01col1", "val1"),
                        makeEntry("02col2", "val2"),
                        makeEntry("03col3", "val3"),
                        makeEntry("04emptycol", ""),
                        makeEntry(COL_END, "valEnd"),
                }; //must be pre-sorted

        Entry[] adds = new Entry[]
                {
                        makeEntry("03col3", "val3-2"), //update?
                        makeEntry("06col6", "val6"), //new val
                        makeEntry("19col19", "val19"), //new val
                }; //must be pre-sorted

        Entry[] dels = new Entry[]
                {
                        makeEntry("01col1", "val1"), //existing val
                        makeEntry("11col11", "val11"), //non-existing val
                }; //must be pre-sorted

        List<BufferPage> mergedPages;

        int maxPageSize = 100;

        BufferPage bp = buildFromEntryArray(entries, entries.length);
        //additions only
        mergedPages =  bp.merge(adds,0,adds.length, new Entry[]{}, 0, 0, maxPageSize);
        assertEquals(1, mergedPages.size());
        assertEquals(entries.length + 2, mergedPages.get(0).numEntries());

        //deletions only
        mergedPages =  bp.merge(new Entry[]{}, 0, 0, dels, 0, dels.length, maxPageSize);
        assertEquals(1, mergedPages.size());
        assertEquals(entries.length - 1, mergedPages.get(0).numEntries());

        //adds+deletes
        mergedPages =  bp.merge(adds,0,adds.length, dels, 0, dels.length, maxPageSize);
        assertEquals(1, mergedPages.size());
        assertEquals(entries.length + 2 - 1, mergedPages.get(0).numEntries());

        //now do the same for smaller page size - should result in multiple pages
        maxPageSize = 3;
        bp = buildFromEntryArray(entries, entries.length);
        mergedPages =  bp.merge(adds,0,adds.length, new Entry[]{}, 0, 0, maxPageSize);
        //we expect 8 entries after merge, so not exactly divisible
        assertEquals(3, mergedPages.size());
        assertEquals(3, mergedPages.get(0).numEntries());
        assertEquals(3, mergedPages.get(1).numEntries());
        assertEquals(2, mergedPages.get(2).numEntries());


        maxPageSize = 2;
        bp = buildFromEntryArray(entries, entries.length);
        mergedPages =  bp.merge(adds,0,adds.length, new Entry[]{}, 0, 0, maxPageSize);
        //we expect 8 entries after merge, exactly divisible
        assertEquals(4, mergedPages.size());
        assertEquals(2, mergedPages.get(0).numEntries());
        assertEquals(2, mergedPages.get(1).numEntries());
        assertEquals(2, mergedPages.get(2).numEntries());
        assertEquals(2, mergedPages.get(3).numEntries());


        //complete removal of page
        mergedPages = mergedPages.get(0).merge(new Entry[]{},0,0,new Entry[]{mergedPages.get(0).getNoCopy(0), mergedPages.get(0).getNoCopy(1)},0,2, maxPageSize);
        assertEquals(0, mergedPages.size());

        maxPageSize = 2;
        bp = buildFromEntryArray(entries, entries.length);
        mergedPages =  bp.merge(adds,0,adds.length-1, new Entry[]{}, 0, 0, maxPageSize);
        //we expect 7 entries after merge
        assertEquals(4, mergedPages.size());
        assertEquals(2, mergedPages.get(0).numEntries());
        assertEquals(2, mergedPages.get(1).numEntries());
        assertEquals(2, mergedPages.get(2).numEntries());
        assertEquals(1, mergedPages.get(3).numEntries());
    }

    @Test
    public void testLargeColumnKey()
    {
        StringBuffer longStringValue = new StringBuffer("-");
        for(int i=0; i < 130; i++)
        {
            longStringValue.append('-');
        }

        assertTrue(longStringValue.length() > 127); //we want to make sure we trigger the switch in valPos storage mode

        Entry[] entries = new Entry[]
        {
            makeEntry("1"+longStringValue.toString(), "qq"),
            makeEntry("2qq", longStringValue.toString()),
            makeEntry("3"+longStringValue.toString(), "qq"),
            makeEntry("4qq", longStringValue.toString()),
        };

        BufferPage bp = buildFromEntryArray(entries, entries.length);

        for (int i=0; i<entries.length; i++)
        {
            assertTrue(bp.get(i).equals(entries[i]));
            assertTrue(bp.getNoCopy(i).equals(entries[i]));
            assertEquals(i, bp.getIndex(entries[i].getColumn()));
        }
    }
}
