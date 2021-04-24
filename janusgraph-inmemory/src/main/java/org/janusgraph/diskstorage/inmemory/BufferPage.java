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
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a single page of a paged column value store, which essentially has the same structure as the original store,
 * i.e. it holds a sorted array of Entries
 * <p>
 * However, instead of storing incoming Entry objects directly, it strips out the raw byte data and stores all entries in a single shared byte array,
 * thus removing the overhead of Entry wrappers and individual byte arrays in each entry.
 *
 * This requires much less heap to store the same data for reasonably well populated store.
 *
 * The maximum saving is achieved when the page is full i.e. has reached the maximum number of entries
 */

class BufferPage {
    public static final int[] EMPTY_INDEX = new int[]{};
    public static final byte[] EMPTY_DATA = new byte[]{};

    private int[] offsetIndex;
    private byte[] rawData;

    BufferPage(final int[] indices, final byte[] data) {
        this.offsetIndex = indices;
        this.rawData = data;
    }

    protected int[] getOffsetIndex() {
        return offsetIndex;
    }

    protected byte[] getRawData() {
        return rawData;
    }

    protected void setRawData(final byte[] rawData) {
        this.rawData = rawData;
    }

    protected void setOffsetIndex(final int[] offsetIndex) {
        this.offsetIndex = offsetIndex;
    }

    public boolean isEmpty() {
        return offsetIndex.length == 0;
    }

    public int getIndex(final StaticBuffer column) {
        //binary search on column names
        int low = 0;
        int high = offsetIndex.length - 1;
        int compare;
        int mid;

        while (low <= high) {
            mid = (low + high) >>> 1;

            //NOTE: we used to do getColumn(mid).compareTo(column), where compareTo internally asserts that column is a StaticARRAYBuffer, so
            // apparently we don't have to worry about column name being in anything but array-backed buffer
            //So we can compare relevant array parts directly, avoiding the churn of new StaticArrayBuffers created by getColumn()
            Preconditions.checkArgument(column instanceof StaticArrayBuffer);
            final int valPos = readValPos(rawData, offsetIndex[mid], null);
            final int valPosSize = BufferPageUtils.computeValPosSize(valPos);
            compare = -((StaticArrayBuffer) column).compareTo(rawData, offsetIndex[mid] + valPosSize, offsetIndex[mid] + valPosSize + valPos);

            if (compare < 0) {
                low = mid + 1;
            } else if (compare > 0) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low + 1);  // key not found.
    }

    private int getEntryLength(final int index) {
        if (index < offsetIndex.length - 1) {
            return offsetIndex[index + 1] - offsetIndex[index];
        } else {
            return rawData.length - offsetIndex[index];
        }
    }

    private int getEntryEndOffset(final int index) {
        if (index < offsetIndex.length - 1) {
            return offsetIndex[index + 1];
        } else {
            return rawData.length;
        }
    }

    /**
     * This figures out how the "valuePosition" was stored (i.e. 1 byte or 4), reads it correctly,
     * and leaves the byte buffer (if given) at the beginning of the key value
     */
    static int readValPos(byte[] rawData, int entryOffset, ByteBuffer entryBuffer) {
        final byte firstByte = rawData[entryOffset];
        if (firstByte > 0) //this entry has a short "column" key whose length fit in 1 byte
        {
            if (entryBuffer != null)
            {
                entryBuffer.position(entryOffset + 1);
            }
            return firstByte;
        }
        else // negative value in first byte indicates that key length doesn't fit in 1 byte - read as whole int and negate to get valuePosition
        {
            final ByteBuffer buf = (entryBuffer == null) ? ByteBuffer.wrap(rawData, entryOffset, rawData.length - entryOffset) : entryBuffer;
            return - buf.getInt();
        }
    }

    /**
     * Entry is packed into byte array in the following way
     * 1. "valPos" - offset at which the value data starts (same as size of "column")
     * 2. "column" - N bytes, where N=valPos
     * 3. "value" - everything from the end of column to the end of the array
     * @param index
     * @return
     */
    public Entry get(final int index) {
        final int entryBufLen = getEntryLength(index);

        final ByteBuffer entryBuffer = ByteBuffer.wrap(rawData, offsetIndex[index], entryBufLen);

        //first read valPos, leave the cursor in the byte buffer at the first byte of "column"
        final int valPos = readValPos(rawData, offsetIndex[index], entryBuffer);

        //now, everything from this position to position+valPos is "column"
        final byte[] col = new byte[valPos];
        entryBuffer.get(col);

        //finally, everything else till the end of the buffer is "value"
        final byte[] val = new byte[entryBufLen - entryBuffer.position() + offsetIndex[index]];
        entryBuffer.get(val);

        return StaticArrayEntry.of(StaticArrayBuffer.of(col), StaticArrayBuffer.of(val));
    }

    public Entry getNoCopy(final int index) {
        final int valPos = readValPos(rawData, offsetIndex[index], null);
        return new StaticArrayEntry(rawData, offsetIndex[index] + BufferPageUtils.computeValPosSize(valPos), getEntryEndOffset(index), valPos);
    }

    public int numEntries() {
        return offsetIndex.length;
    }

    public int byteSize() {
        return rawData.length + offsetIndex.length * Integer.BYTES + 16;
    }

    public static List<BufferPage> merge(List<BufferPage> pagesToMerge, int maxPageSize) {
        if (pagesToMerge == null || pagesToMerge.size() < 1) {
            return pagesToMerge;
        }

        //NOTE: instead of constructing all Entries and then rebuilding offsetIndex and rawData,
        // it could use arraycopy to copy parts of existing buffers into new ones
        // it is possible to do in one pass, but rather complex as we'd need to keep track of current offset in new buffer,
        // current page and offset within that page etc. In practice, this is required seldom enough to create any noticeable deficiencies.

        // Just extracts all Entries from all fragmented pages into a single list, then create new full pages
        int totalCount = 0;
        for (BufferPage p : pagesToMerge) {
            totalCount += p.numEntries();
        }

        Entry[] newdata = new Entry[totalCount];

        totalCount = 0;
        for (BufferPage p : pagesToMerge) {
            for (int i = 0; i < p.numEntries(); i++) {
                newdata[totalCount++] = p.getNoCopy(i);
            }
        }

        int numNewPages = newdata.length / maxPageSize;
        if (newdata.length % maxPageSize > 0) {
            numNewPages++;
        }

        List<BufferPage> newPages = new ArrayList<>(numNewPages);
        for (int i = 0; i < numNewPages; i++) {
            newPages.add(BufferPageUtils.buildFromEntryArray(newdata, i * maxPageSize, Math.min(newdata.length, (i + 1) * maxPageSize)));
        }

        return newPages;
    }

    public List<BufferPage> merge(Entry[] add, int iaddp, int addLimit, Entry[] del, int idelp, int delLimit, int maxPageSize) {
        int iadd = iaddp;
        int idel = idelp;
        Entry[] newdata = new Entry[numEntries() + addLimit - iadd];

        // Merge sort
        int i = 0;
        int iold = 0;
        while (iold < numEntries()) {
            Entry e = getNoCopy(iold);
            iold++;
            // Compare with additions
            if (iadd < addLimit) {
                int compare = e.compareTo(add[iadd]);
                if (compare >= 0) {
                    e = add[iadd];
                    iadd++;
                    // Skip duplicates
                    while (iadd < addLimit && e.equals(add[iadd])) {
                        iadd++;
                    }
                }
                if (compare > 0) {
                    iold--;
                }
            }
            // Compare with deletions
            if (idel < delLimit) {
                int compare = e.compareTo(del[idel]);
                if (compare == 0) {
                    e = null;
                }
                if (compare >= 0) {
                    idel++;
                }
            }
            if (e != null) {
                newdata[i] = e;
                i++;
            }
        }
        while (iadd < addLimit) {
            newdata[i] = add[iadd];
            i++;
            iadd++;
        }

        int newDataEnd = i;
        int numNewPages = newDataEnd / maxPageSize;
        if (newDataEnd % maxPageSize > 0) {
            numNewPages++;
        }

        List<BufferPage> newPages = new ArrayList<>(numNewPages);

        for (i = 0; i < numNewPages; i++) {
            newPages.add(BufferPageUtils.buildFromEntryArray(newdata, i * maxPageSize, Math.min(newDataEnd, (i + 1) * maxPageSize)));
        }

        return newPages;
    }
}
