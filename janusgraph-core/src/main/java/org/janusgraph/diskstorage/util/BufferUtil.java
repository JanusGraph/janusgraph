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

package org.janusgraph.diskstorage.util;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.idhandling.VariableString;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.util.encoding.StringEncoding;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.janusgraph.graphdb.database.idhandling.IDHandler.STOP_MASK;

/**
 * Utility methods for dealing with {@link ByteBuffer}.
 *
 */
public class BufferUtil {

    public static final int longSize = StaticArrayBuffer.LONG_LEN;
    public static final int intSize = StaticArrayBuffer.INT_LEN;
    public static final int charSize = StaticArrayBuffer.CHAR_LEN;
    public static final int byteSize = StaticArrayBuffer.BYTE_LEN;

    /* ###############
     * Simple StaticBuffer construction
     * ################
     */

    public static StaticBuffer getIntBuffer(int id) {
        ByteBuffer buffer = ByteBuffer.allocate(intSize);
        buffer.putInt(id);
        byte[] arr = buffer.array();
        Preconditions.checkArgument(arr.length == intSize);
        return StaticArrayBuffer.of(arr);
    }

    public static StaticBuffer getIntBuffer(int[] ids) {
        ByteBuffer buffer = ByteBuffer.allocate(intSize * ids.length);
        for (int id : ids) buffer.putInt(id);
        byte[] arr = buffer.array();
        Preconditions.checkArgument(arr.length == intSize * ids.length);
        return StaticArrayBuffer.of(arr);
    }

    /**
     * We always add a byte marker at the beginning to indicate it's a string buffer. Then we
     * write the string uncompressed. Finally, if the total buffer size is 8, we add a dummy byte
     * at the end so that upon read time, JanusGraph knows the buffer does not store a long value.
     * See {@link IDManager#getKeyID(StaticBuffer)} for more details.
     *
     * Note that we apply STOP_MASK to the last character. We don't have to do it because a static buffer
     * has a fixed length, and thus upon read time, we know where to stop reading the string. This is more
     * to keep it consistent with {@link StringEncoding#writeAsciiString(byte[], int, String)} where we use
     * STOP_MASK to mark the end of the string. An additional benefit of doing so is to enable corruption check
     * in the future. Note: this approach has no overhead.
     *
     * @param s
     * @return
     */
    public static StaticBuffer getStringIdBuffer(String s) {
        VariableString.checkAsciiPrintableString(s);
        WriteBuffer buffer;
        if (s.length() + byteSize == longSize) {
            buffer = new WriteByteBuffer(longSize + byteSize);
        } else {
            buffer = new WriteByteBuffer(s.length() + byteSize);
        }
        VariableString.write(buffer, s);

        if (s.length() + byteSize == longSize) {
            // this could be any dummy byte, so we just use STOP_MASK
            buffer.putByte(STOP_MASK);
        }
        return buffer.getStaticBuffer();
    }

    public static StaticBuffer getLongBuffer(long id) {
        ByteBuffer buffer = ByteBuffer.allocate(longSize);
        buffer.putLong(id);
        byte[] arr = buffer.array();
        Preconditions.checkArgument(arr.length == longSize);
        return StaticArrayBuffer.of(arr);
    }

    public static StaticBuffer fillBuffer(int len, byte value) {
        byte[] res = new byte[len];
        for (int i = 0; i < len; i++) res[i]=value;
        return StaticArrayBuffer.of(res);
    }

    public static StaticBuffer oneBuffer(int len) {
        return fillBuffer(len,(byte)-1);
    }

    public static StaticBuffer zeroBuffer(int len) {
        return fillBuffer(len,(byte)0);
    }

    public static StaticBuffer emptyBuffer() {
        return fillBuffer(0,(byte)0);
    }

    /* ################
     * Buffer I/O
     * ################
     */

    public static void writeEntry(DataOutput out, Entry entry) {
        VariableLong.writePositive(out,entry.getValuePosition());
        writeBuffer(out,entry);
        if (!entry.hasMetaData()) out.putByte((byte)0);
        else {
            Map<EntryMetaData,Object> metadata = entry.getMetaData();
            assert metadata.size()>0 && metadata.size()<Byte.MAX_VALUE;
            assert EntryMetaData.values().length<Byte.MAX_VALUE;
            out.putByte((byte)metadata.size());
            for (Map.Entry<EntryMetaData,Object> metas : metadata.entrySet()) {
                EntryMetaData meta = metas.getKey();
                out.putByte((byte)meta.ordinal());
                out.writeObjectNotNull(metas.getValue());
            }
        }
    }

    public static void writeBuffer(DataOutput out, StaticBuffer buffer) {
        VariableLong.writePositive(out,buffer.length());
        out.putBytes(buffer);
    }

    public static Entry readEntry(ReadBuffer in, Serializer serializer) {
        long valuePosition = VariableLong.readPositive(in);
        Preconditions.checkArgument(valuePosition>0 && valuePosition<=Integer.MAX_VALUE);
        StaticBuffer buffer = readBuffer(in);

        StaticArrayEntry entry = new StaticArrayEntry(buffer, (int) valuePosition);
        int metaSize = in.getByte();
        for (int i=0;i<metaSize;i++) {
            EntryMetaData meta = EntryMetaData.values()[in.getByte()];
            entry.setMetaData(meta,serializer.readObjectNotNull(in,meta.getDataType()));
        }
        return entry;
    }

    public static StaticBuffer readBuffer(ScanBuffer in) {
        long length = VariableLong.readPositive(in);
        Preconditions.checkArgument(length>=0 && length<=Integer.MAX_VALUE);
        byte[] data = in.getBytes((int)length);
        assert data.length==length;
        return new StaticArrayBuffer(data);
    }

    /* ################
     * StaticBuffer Manipulation
     * ################
     */

    public static StaticBuffer padBuffer(StaticBuffer b, int length) {
        if (b.length()>=length) return b;
        byte[] data = new byte[length]; //implicitly initialized to all 0s
        for (int i = 0; i < b.length(); i++) {
            data[i]=b.getByte(i);
        }
        return new StaticArrayBuffer(data);
    }

    public static StaticBuffer nextBiggerBufferAllowOverflow(StaticBuffer buffer) {
        return nextBiggerBuffer(buffer, true);
    }

    public static StaticBuffer nextBiggerBuffer(StaticBuffer buffer) {
        return nextBiggerBuffer(buffer,false);
    }

    private static StaticBuffer nextBiggerBuffer(StaticBuffer buffer, boolean allowOverflow) {
        int len = buffer.length();
        byte[] next = new byte[len];
        boolean carry = true;
        for (int i = len - 1; i >= 0; i--) {
            byte b = buffer.getByte(i);
            if (carry) {
                b++;
                if (b != 0) carry = false;
            }
            next[i]=b;
        }
        if (carry && allowOverflow) {
            return zeroBuffer(len);
        } else if (carry) {
            throw new IllegalArgumentException("Buffer overflow incrementing " + buffer);
        } else {
            return StaticArrayBuffer.of(next);
        }

    }

    /**
     * Thread safe equals method for StaticBuffer - ByteBuffer equality comparison
     *
     * @param b1
     * @param b2
     * @return
     */
    public static boolean equals(StaticBuffer b1, ByteBuffer b2) {
        if (b1.length()!=b2.remaining()) return false;
        int p2 = b2.position();
        for (int i=0;i<b1.length();i++) {
            if (b1.getByte(i)!=b2.get(p2+i)) return false;
        }
        return true;
    }


}
