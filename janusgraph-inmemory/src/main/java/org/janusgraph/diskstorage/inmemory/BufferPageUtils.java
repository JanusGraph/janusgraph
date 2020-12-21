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
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a collection of static methods to help read a BufferPage from a stream it was previously dumped to
 */
public final class BufferPageUtils {

    private static final Logger log = LoggerFactory.getLogger(BufferPageUtils.class);

    public static BufferPage readPage(DataInputStream in) throws IOException {
        int numEntries = in.readInt();
        if (log.isDebugEnabled()) {
            log.debug("page numentries is " + numEntries);
        }

        if (numEntries > 0) {
            int[] index = new int[numEntries];
            //NOTE: again, no way to read int[] as a block of bytes apparently... relying on buffering
            for (int i = 0; i < index.length; i++) {
                index[i] = in.readInt();
            }

            int dataLength = in.readInt();
            if (log.isDebugEnabled()) {
                log.debug("page data size is " + dataLength);
            }

            byte[] data = new byte[dataLength];
            readWholeArray(in, data);

            return new BufferPage(index, data);
        } else
            return new BufferPage(BufferPage.EMPTY_INDEX, BufferPage.EMPTY_DATA);
    }

    public static void readWholeArray(DataInputStream in, byte[] data) throws IOException {
        int offset = 0;
        do {
            int bytesRead = in.read(data, offset, data.length - offset);
            if (bytesRead < 0) {
                throw new IllegalStateException("Premature end of file while reading, expected to read " + data.length);
            }
            offset += bytesRead;
        }
        while (offset < data.length);
    }

    public static SharedEntryBuffer readFrom(DataInputStream in) throws IOException {
        int numPages = in.readInt();

        if (log.isDebugEnabled()) {
            log.debug("number of pages in column store is " + numPages);
        }

        if (numPages == 1) {
            BufferPage p = readPage(in);

            return new SinglePageEntryBuffer(p.getOffsetIndex(), p.getRawData());
        } else {
            List<BufferPage> pages = new ArrayList<>(numPages);
            for (int i = 0; i < numPages; i++) {
                BufferPage p = readPage(in);
                pages.add(p);
            }
            return new MultiPageEntryBuffer(pages);
        }
    }

    static int computeValPosSize(int valuePosition)
    {
        //this assumes that the key size will almost never be > 127 bytes,
        // thus most of the time saving 3 out of 4 bytes to store the value position within the buffer
        return valuePosition > 127 ? Integer.BYTES : 1;
    }

    static int writeValPos(Entry e, byte[] rawData, int offset)
    {
        if (e.getValuePosition() <= 127) //"column" name length fits into one byte - should be 99.99% of cases
        {
            final byte entryValPos = (byte) e.getValuePosition();
            rawData[offset] = entryValPos;
            return 1;
        }
        else //doesn't fit in 1 byte - use full integer
        {
            // if we write -valuePosition in big-endian order, 1st byte should have a negative value, indicating full int
            ByteBuffer.wrap(rawData, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).asIntBuffer().put(-e.getValuePosition());
            return Integer.BYTES;
        }
    }

    public static BufferPage buildFromEntryArray(final Entry[] array, final int start, final int end) {
        Preconditions.checkArgument(start >= 0 && end <= array.length);
        final int size = end - start;

        //compute the size of the raw byte storage and the offsets of individual entries inside it
        int[] offsetIndex = new int[size];
        int rawSize = 0;
        for (int i = 0; i < size; i++) {
            offsetIndex[i] = rawSize;
            rawSize += array[start + i].length() + computeValPosSize(array[start+i].getValuePosition()); //extra bytes to store valuePosition
        }
        byte[] rawData = new byte[rawSize];

        for (int i = 0; i < size; i++) {
            final int valPosSize = writeValPos(array[start + i], rawData, offsetIndex[i]);

            if (array[start + i] instanceof StaticArrayBuffer) {
                final StaticArrayBuffer arrayBuffer = (StaticArrayBuffer) array[start + i];
                arrayBuffer.copyTo(rawData, offsetIndex[i] + valPosSize);
            } else {
                final byte[] entryData = array[start + i].getBytes(0, array[start + i].length());
                System.arraycopy(entryData, 0, rawData, offsetIndex[i] + valPosSize, entryData.length);
            }
        }
        return new BufferPage(offsetIndex, rawData);
    }

    public static BufferPage buildFromEntryArray(final Entry[] array, final int size) {
        return buildFromEntryArray(array, 0, size);
    }

    public static void dumpTo(BufferPage page, DataOutputStream out) throws IOException {
        //page dump format is: index size aka numEntries, (offsetIndex, dataSize, rawData -- if numentries > 0)
        if (log.isDebugEnabled()) {
            log.debug("page index numEntries is " + page.getOffsetIndex().length + ", byte size is " + page.getOffsetIndex().length * Integer.BYTES);
        }
        out.writeInt(page.getOffsetIndex().length);

        if (page.getOffsetIndex().length > 0) {
            //NOTE: looks like there is no other way to write out an int[] without copying it?.. no IntBuffer.asByteBuffer(endianness) or smth...
            // will rely on buffering here
            for (int i : page.getOffsetIndex()) {
                out.writeInt(i);
            }

            out.writeInt(page.getRawData().length);
            out.write(page.getRawData());
            if (log.isDebugEnabled()) {
                log.debug("page data length is " + page.getRawData().length + ", byte size is " + page.getRawData().length * Integer.BYTES);
            }
        }
    }
}
