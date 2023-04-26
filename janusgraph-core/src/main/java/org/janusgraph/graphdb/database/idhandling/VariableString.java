// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.graphdb.database.idhandling;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.serialize.attribute.StringSerializer;

import static org.janusgraph.graphdb.database.idhandling.IDHandler.STOP_MASK;
import static org.janusgraph.graphdb.database.idhandling.IDHandler.STRING_ID_MARKER;


/**
 * Handle String serialization and deserialization, support both forward and backward read and write.
 * This does not use any compression technique, so it is most suitable for short string, e.g. vertex
 * id.
 *
 * Important: it uses {@link IDHandler#STRING_ID_MARKER} as the first byte. This is because JanusGraph used
 * to assume all IDs are positive long values. To maintain backward compatibility, we have to make sure
 * JanusGraph knows how to distinguish between string ID and long ID when decoding binary data. The
 * convention is, if the first byte is STRING_MARKER, then it's a string ID, otherwise it's a long ID. This
 * leverages the fact that IDs are always positive, so the first byte would never be STRING_MARKER for long
 * IDs.
 *
 * This class uses {@link StringSerializer} as a reference.
 *
 * @author Boxuan Li (liboxuan@connect.hku.hk)
 */
public class VariableString {

    public static void checkAsciiPrintableString(final String value) {
        if (StringUtils.isEmpty(value) || !StringUtils.isAsciiPrintable(value)) {
            throw new IllegalArgumentException("Value must be non-empty printable ASCII string!");
        }
    }

    public static void write(WriteBuffer out, final String value) {
        checkAsciiPrintableString(value);
        out.putByte(STRING_ID_MARKER);
        for (int i = 0; i < value.length(); i++) {
            int c = value.charAt(i);
            assert c <= 127;
            byte b = (byte)c;
            if (i+1==value.length()) b |= STOP_MASK; //End marker
            out.putByte(b);
        }
    }

    public static void writeBackward(WriteBuffer out, final String value) {
        checkAsciiPrintableString(value);
        for (int i = value.length() - 1; i >= 0; i--) {
            int c = value.charAt(i);
            assert c <= 127;
            byte b = (byte)c;
            if (i == value.length() - 1) b |= STOP_MASK; //End marker
            out.putByte(b);
        }
        out.putByte(STRING_ID_MARKER);
    }

    public static String read(ReadBuffer in) {
        Preconditions.checkArgument(in.getByte() == STRING_ID_MARKER);
        StringBuilder sb = new StringBuilder();
        while (true) {
            int c = 0xFF & in.getByte();
            sb.append((char)(c & 0x7F));
            if ((c & 0x80) > 0) break;
        }
        return sb.toString();
    }

    public static String readBackward(ReadBuffer in) {
        int position = in.getPosition();
        Preconditions.checkArgument(in.getByte(--position) == STRING_ID_MARKER);
        StringBuilder sb = new StringBuilder();
        while (true) {
            int c = 0xFF & in.getByte(--position);
            sb.append((char)(c & 0x7F));
            if ((c & 0x80) > 0) break;
        }
        in.movePositionTo(position);
        return sb.toString();
    }
}
