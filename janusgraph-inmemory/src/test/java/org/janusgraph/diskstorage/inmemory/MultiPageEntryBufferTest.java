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
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.junit.jupiter.api.Test;

import static org.janusgraph.diskstorage.inmemory.BufferPageTest.makeStaticBuffer;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiPageEntryBufferTest
{

    @Test
    public void testBuffer() throws Exception
    {
        SinglePageEntryBuffer singlePage = new SinglePageEntryBuffer();
        MultiPageEntryBuffer buffer = new MultiPageEntryBuffer(singlePage);

        int maxPageSize = 50;

        Entry[] empty = new Entry[0];

        Entry[] add1 = InMemoryColumnValueStoreTest.generateEntries(0,123,"qq").toArray(empty);
        Entry[] add2 = InMemoryColumnValueStoreTest.generateEntries(113,347,"qq").toArray(empty);
        Entry[] add3 = InMemoryColumnValueStoreTest.generateEntries(340,497,"qq").toArray(empty);
        Entry[] del1 = new Entry[] {};
        Entry[] del2 = InMemoryColumnValueStoreTest.generateEntries(3,43,"qq").toArray(empty);
        Entry[] del3 = InMemoryColumnValueStoreTest.generateEntries(55,99,"qq").toArray(empty);
        Entry[] del4 = InMemoryColumnValueStoreTest.generateEntries(415,446,"qq").toArray(empty);
        Entry[] del5 = InMemoryColumnValueStoreTest.generateEntries(453,495,"qq").toArray(empty);

        buffer.mutate(add1, del1, maxPageSize);

        assertEquals(3, buffer.numPages());

        buffer.getSlice(new KeySliceQuery(makeStaticBuffer("someRow"),
                                          makeStaticBuffer(InMemoryColumnValueStoreTest.VERY_START),
                                          makeStaticBuffer(InMemoryColumnValueStoreTest.VERY_END)));

        buffer.mutate(add2, del1, maxPageSize);
        assertEquals(7, buffer.numPages());
        buffer.mutate(add3, del1, maxPageSize);
        assertEquals(10, buffer.numPages());
        buffer.mutate(new Entry[]{}, del2, maxPageSize);
        assertEquals(10, buffer.numPages());
        buffer.mutate(new Entry[]{}, del3, maxPageSize);
        assertEquals(10, buffer.numPages());
        buffer.mutate(new Entry[]{}, del4, maxPageSize);
        assertEquals(10, buffer.numPages());
        buffer.mutate(new Entry[]{}, del5, maxPageSize);
        assertEquals(10, buffer.numPages());


        buffer.isEmpty();
        buffer.isPaged();
        buffer.numEntries();
        buffer.createFragmentationReport(maxPageSize);
        buffer.quickDefragment(maxPageSize);
        assertEquals(8, buffer.numPages());
        buffer.createFragmentationReport(maxPageSize);
    }
}
