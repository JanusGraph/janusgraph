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
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.janusgraph.diskstorage.inmemory.BufferPageUtils.buildFromEntryArray;

/**
 * "Multi-page" implementation of SharedEntryBuffer.
 * Maintains a list of pages, each containing a shared byte buffer for maxPageSize Entries. Dividing the buffer into
 * pages helps maintain performance in the case of very big stores. This is because when mutating, instead of re-copying
 * all the entries in a single byte buffer, we only locate and modify a few pages, skipping those not affected by the mutation.
 * <p>
 * Since only a limited number of stores actually get very big, the parent store switches from SinglePageEntryBuffer
 * to this implementation when a mutation brings the max number of entries beyond maxPageSize threshold.
 */
public class MultiPageEntryBuffer implements SharedEntryBuffer {
    private static int INITIAL_CAPACITY = 2;

    private static final Logger log = LoggerFactory.getLogger(BufferPageUtils.class);

    //NOTE: tracking pages in a list, which internally maintains an array, creates additional overhead;
    // if we maintain our page list in an array directly, this will save this overhead at the cost of some more boilerplate code here
    // However in practice the number of multi-page buffers is usually relatively small, so at the moment it seems to be
    // a reasonable price for having less code
    private ArrayList<BufferPage> pages;

    public MultiPageEntryBuffer(BufferPage initialPage) {
        this.pages = new ArrayList<>(INITIAL_CAPACITY);
        if (initialPage instanceof SinglePageEntryBuffer) {
            pages.add(new BufferPage(initialPage.getOffsetIndex(), initialPage.getRawData()));
        } else
            pages.add(initialPage);
    }

    MultiPageEntryBuffer(List<BufferPage> allPages) {
        this.pages = new ArrayList<>(allPages);
    }

    @Override
    public int numPages() {
        return pages.size();
    }

    @Override
    public int numEntries() {
        if (pages == null) {
            return 0;
        }

        int numEntries = 0;

        for (BufferPage p : pages) {
            numEntries += p.numEntries();
        }
        return numEntries;
    }

    @Override
    public int byteSize() {
        if (pages == null) {
            return 0;
        }

        int byteSize = 0;
        for (BufferPage p : pages) {
            byteSize += p.byteSize() + 16;
        }
        return byteSize;
    }

    @Override
    public boolean isEmpty() {
        return pages.isEmpty();
    }

    private int getPageIndex(StaticBuffer column) {
        //binary search for correct page on column name
        int low = 0;
        int high = pages.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            BufferPage midPage = pages.get(mid);

            int idx = midPage.getIndex(column);

            if (idx >= 0) {
                return mid; //key found in this page
            }

            //key not found, idx contains nearest index, negated
            idx = (-idx - 1);

            if (idx <= 0) //key is smaller than the first key in this page - search previous pages
            {
                high = mid - 1;
            } else if (idx >= midPage.numEntries()) //key is bigger than the last in this page - search following pages
            {
                low = mid + 1;
            } else //key was not found in this page, but the nearest key found is in this page
            {
                return mid;
            }
        }
        return -(low + 1);  //key not found in any of the pages, and is outside the range of current pages - return the nearest page
    }

    @Override
    public EntryList getSlice(KeySliceQuery query) {
        if (pages == null || pages.size() == 0) {
            return EntryList.EMPTY_LIST;
        }

        int startPageIdx = getPageIndex(query.getSliceStart());
        if (startPageIdx < 0) {
            startPageIdx = (-startPageIdx - 1);
        }

        int endPageLimit = getPageIndex(query.getSliceEnd());
        if (endPageLimit < 0) {
            endPageLimit = (-endPageLimit - 1);
        } else {
            endPageLimit++; //we want "insertion point" like in case when it wasn't found
        }

        if (startPageIdx < endPageLimit) {
            int startIndex = pages.get(startPageIdx).getIndex(query.getSliceStart());
            if (startIndex < 0) {
                startIndex = (-startIndex - 1);
            }

            int endIndex = pages.get(endPageLimit - 1).getIndex(query.getSliceEnd());
            if (endIndex < 0) {
                endIndex = (-endIndex - 1);
            }

            MemoryEntryList result = new MemoryEntryList(0);
            for (int i = startPageIdx; i < endPageLimit; i++) {
                if (query.hasLimit() && result.size() >= query.getLimit()) {
                    break;
                }

                BufferPage page = pages.get(i);
                int start = (i == startPageIdx) ? startIndex : 0;
                int end = (i == endPageLimit - 1) ? endIndex : page.numEntries();
                for (int j = start; j < end; j++) {
                    if (query.hasLimit() && result.size() >= query.getLimit()) {
                        break;
                    }

                    //using getNoCopy here means the old page buffer will not be collected until the last of the Entries it used to hold is collected
                    //however, since all Entries we return are managed by a single transaction and not presented to user code, this should not be a problem
                    result.add(page.getNoCopy(j));
                }
            }

            return result;
        } else {
            return EntryList.EMPTY_LIST;
        }
    }

    @Override
    public void mutate(Entry[] add, Entry[] del, int maxPageSize) {
        int pageHits = 0;
        int oldPageCount = pages.size();

        // Merge sort:
        //  iterate through pages,
        //  skip those pages which are not hit by any adds or deletes,
        //  rebuild those which are hit, merging existing data with all adds/deletes up to next page margin
        //  if new page is going to hit max size - insert new one
        if (pages.size() == 0) {
            pages.add(buildFromEntryArray(new Entry[]{}, 0));
        }

        int iadd = 0;
        int idel = 0;
        //NOTE: if it finds min of first add/first delete via getPageIndex (binary search), jumps straight to that page
        // - could be better for big stores updated sparsely. However in practice it doesn't seem to be any noticeable bottleneck
        int currPageNo = 0;

        while (currPageNo < pages.size() && (iadd < add.length || idel < del.length)) {
            BufferPage currPage = pages.get(currPageNo);
            BufferPage nextPage = (currPageNo + 1 < pages.size()) ? pages.get(currPageNo + 1) : null;

            //assumes there will be no pages with zero entries - i.e. we will delete a page if it contains no data
            Preconditions.checkArgument(nextPage == null || nextPage.numEntries() > 0);
            StaticBuffer nextPageStart = nextPage == null ? null : nextPage.getNoCopy(0);

            boolean pageNeedsMerging = false;

            // Compare with additions
            if (iadd < add.length) //still have things to add
            {
                pageNeedsMerging = nextPageStart == null; //if there's no next page then we definitely need to merge into this page

                if (!pageNeedsMerging) {
                    //if next page start is bigger than the key we need to add, this means we need to merge this page
                    //if next page start is smaller, then we can skip this page - we will merge one of the next pages we see
                    int compare = nextPageStart.compareTo(add[iadd]);
                    pageNeedsMerging = compare >= 0;
                }
            }
            // Compare with deletions
            if (!pageNeedsMerging && idel < del.length) //still have things to delete, and still not sure if we need to merge this page
            {
                //if this page end is bigger than the key we need to delete, this means we need to merge this page
                //if it is smaller, then we won't find anything to delete in this page anyway
                StaticBuffer thisPageEnd = currPage.getNoCopy(currPage.numEntries() - 1);
                int compare = thisPageEnd.compareTo(del[idel]);
                pageNeedsMerging = compare >= 0;
            }

            if (pageNeedsMerging) {
                int addLimit;
                int delLimit;
                if (nextPageStart == null) //this is the last page, everything we still need to add/delete applies to it
                {
                    addLimit = add.length;
                    delLimit = del.length;
                } else //this is not the last page, we need to determine which adds/deletes go to this page, and which go to next page(s)
                {
                    //NOTE: for long mutation lists, it could be better to do binary search here,
                    // otherwise it could be up to maxPageSize linear comparisons.
                    // However it was not seen as a bottleneck in practice so far
                    addLimit = iadd;
                    while (addLimit < add.length && nextPageStart.compareTo(add[addLimit]) > 0) {
                        addLimit++;
                    }

                    delLimit = idel;
                    while (delLimit < del.length && nextPageStart.compareTo(del[delLimit]) > 0) {
                        delLimit++;
                    }
                }

                List<BufferPage> mergedPages = currPage.merge(add, iadd, addLimit, del, idel, delLimit, maxPageSize);
                if (mergedPages.size() == 0) //there was no data left in the page as a result of merge - remove old page
                {
                    pages.remove(currPageNo);
                    //do NOT increase currPageNo here as the next page moved in to this place
                } else //there is at least one page as a result of merge - replace the current one and insert any additional overflow pages
                {
                    pages.set(currPageNo, mergedPages.get(0)); //replace the currPage with the newly merged version
                    currPageNo++; //move to next page
                    if (mergedPages.size() > 1) //more than one page as a result of merge - insert all additional ones
                    {
                        mergedPages.remove(0);
                        pages.addAll(currPageNo, mergedPages);

                        //skip over the pages we just added as they cannot contain any work we might still need to do
                        currPageNo += mergedPages.size();
                        pageHits += mergedPages.size();
                    }
                }

                iadd = addLimit;
                idel = delLimit;

                pageHits++;
            } else {
                currPageNo++;
            }
        }

        if (oldPageCount >= pages.size()) {
            //it grew before but not this time, assume it stopped growing for now and trim to size to save memory
            pages.trimToSize();
        }
    }

    @Override
    public boolean isPaged() {
        return true;
    }

    @Override
    public SharedEntryBufferFragmentationReport createFragmentationReport(int maxPageSize) {
        SharedEntryBufferFragmentationReport.Builder report = new SharedEntryBufferFragmentationReport.Builder();

        report.pageCount(pages.size());
        if (pages.size() < 2) {
            //not fragmented;
            return report.build();
        }
        //We try to identify chunks of 2 or more adjacent non-full pages.
        //Such chunks are easily "compactable" by merging into one or more full pages.
        //this is different from doing "full defragmentation", which would require moving contents of full pages as well
        int compressableChunksCount = 0;
        int fragmentedPageCount = 0;
        int currCompressableCount = 0;
        int totalCompressableCount = 0;
        int currChunkNumEntries = 0;
        int achievablePageReduction = 0;
        for (BufferPage page : pages) {
            if (page.numEntries() < maxPageSize) {
                fragmentedPageCount++;
                currCompressableCount++;
                currChunkNumEntries += page.numEntries();
            } else {
                int numPagesAfterCompression = getNumPagesRequired(currChunkNumEntries, maxPageSize);
                if (numPagesAfterCompression < currCompressableCount) {
                    compressableChunksCount++;
                    totalCompressableCount += currCompressableCount;
                    achievablePageReduction += currCompressableCount - numPagesAfterCompression;
                }
                currCompressableCount = 0;
                currChunkNumEntries = 0;
            }
        }

        if (currCompressableCount > 1) {
            compressableChunksCount++;
            totalCompressableCount += currCompressableCount;
            achievablePageReduction += currCompressableCount - getNumPagesRequired(currChunkNumEntries, maxPageSize);
        }

        return report.fragmentedPageCount(fragmentedPageCount)
            .compressableChunksCount(compressableChunksCount)
            .compressablePageCount(totalCompressableCount)
            .achievablePageReduction(achievablePageReduction).build();
    }

    @Override
    public void quickDefragment(int maxPageSize) {
        List<BufferPage> currChunkToDefragment = new ArrayList<>();
        int currChunkStart = -1;
        int currChunkNumEntries = 0;
        for (int i = 0; i < pages.size(); i++) {
            BufferPage page = pages.get(i);
            if (page.numEntries() < maxPageSize) {
                if (currChunkStart < 0) {
                    currChunkStart = i;
                }
                currChunkNumEntries += page.numEntries();
                currChunkToDefragment.add(page);
            } else {
                //if we can get less pages back by compacting the current chunk then proceed
                if (currChunkToDefragment.size() > 1 && getNumPagesRequired(currChunkNumEntries, maxPageSize) < currChunkToDefragment.size()) {
                    defragmentAndReplaceChunk(maxPageSize, currChunkToDefragment, currChunkStart);
                }

                currChunkToDefragment.clear();
                currChunkStart = -1;
                currChunkNumEntries = 0;
            }
        }
        if (currChunkToDefragment.size() > 1 && getNumPagesRequired(currChunkNumEntries, maxPageSize) < currChunkToDefragment.size()) {
            defragmentAndReplaceChunk(maxPageSize, currChunkToDefragment, currChunkStart);
        }
    }

    @Override
    public void dumpTo(DataOutputStream out) throws IOException {
        //the dump format is: numPages, (pagedump)*

        if (log.isDebugEnabled()) {
            log.debug("number of pages in multipage buffer is " + numPages());
        }
        out.writeInt(numPages());

        for (BufferPage page : pages) {
            BufferPageUtils.dumpTo(page, out);
        }
    }

    private int getNumPagesRequired(int currChunkNumEntries, int maxPageSize) {
        return currChunkNumEntries / maxPageSize + (currChunkNumEntries % maxPageSize > 0 ? 1 : 0);
    }

    private void defragmentAndReplaceChunk(int maxPageSize, List<BufferPage> currChunkToDefragment, int currChunkStart) {
        List<BufferPage> mergedPages = BufferPage.merge(currChunkToDefragment, maxPageSize);
        for (int j = currChunkToDefragment.size() - 1; j >= 0; j--) {
            pages.remove(currChunkStart + j);
        }
        pages.addAll(currChunkStart, mergedPages);
    }
}
