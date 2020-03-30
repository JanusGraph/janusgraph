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

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * "Single page" implementation of SharedEntryBuffer.
 * <p>
 * It extends BufferPage and complements it with the methods required to make a page look like a complete buffer.
 * In essence, it just makes a page mutable by reassigning merge results to the same buffer + index refs
 * <p>
 * This saves a lot of overhead because 90% of stores fit into one page, as we don't have to keep track of a list of pages etc
 */
public class SinglePageEntryBuffer extends BufferPage implements SharedEntryBuffer {

    private static final Logger log = LoggerFactory.getLogger(BufferPageUtils.class);

    public SinglePageEntryBuffer() {
        super(EMPTY_INDEX, EMPTY_DATA);
    }

    SinglePageEntryBuffer(final int[] index, final byte[] data) {
        super(index, data);
    }

    @Override
    public int numPages() {
        return 1;
    }

    @Override
    public EntryList getSlice(KeySliceQuery query) {
        int start = getIndex(query.getSliceStart());
        if (start < 0) {
            start = (-start - 1);
        }
        int end = getIndex(query.getSliceEnd());
        if (end < 0) {
            end = (-end - 1);
        }
        if (start < end) {
            MemoryEntryList result = new MemoryEntryList(end - start);
            for (int i = start; i < end; i++) {
                if (query.hasLimit() && result.size() >= query.getLimit()) {
                    break;
                }

                //using getNoCopy here as well, means the old common buffer will not be collected until the last of the Entries it used to hold is collected
                //however, since all Entries in an array are managed by a single transaction and not presented to user code, this should not be a problem
                result.add(getNoCopy(i));
            }
            return result;
        } else {
            return EntryList.EMPTY_LIST;
        }
    }

    @Override
    public void mutate(Entry[] add, Entry[] del, int maxPageSize) {
        //call "immutable" merge method with "unlimited" max page size (ignoring the one passed), to obtain new offset and rawData arrays
        List<BufferPage> res = merge(add, 0, add.length, del, 0, del.length, Integer.MAX_VALUE);

        //now reassign this instance's index and rawData
        if (res.isEmpty()) {
            this.setOffsetIndex(EMPTY_INDEX);
            this.setRawData(EMPTY_DATA);
        } else {
            Preconditions.checkArgument(res.size() == 1); //page size is "unlimited" so we should get exactly 1 page
            final BufferPage resPage = res.get(0);

            this.setRawData(resPage.getRawData());
            this.setOffsetIndex(resPage.getOffsetIndex());
        }
    }

    @Override
    public boolean isPaged() {
        return false;
    }

    @Override
    public SharedEntryBufferFragmentationReport createFragmentationReport(int maxPageSize) {
        return new SharedEntryBufferFragmentationReport.Builder().pageCount(1).build();
        //rest is 0 by default
    }

    @Override
    public void quickDefragment(int maxPageSize) {
        //do nothing - single buffer is not fragmented by construction
    }

    @Override
    public void dumpTo(DataOutputStream out) throws IOException {
        //the dump format is: numPages, (pagedump)*
        if (log.isDebugEnabled()) {
            log.debug("number of pages in single page buffer is " + numPages());
        }
        out.writeInt(numPages());

        BufferPageUtils.dumpTo(this, out); //call dumpTo for Page since this is basically a single page anyway
    }
}
