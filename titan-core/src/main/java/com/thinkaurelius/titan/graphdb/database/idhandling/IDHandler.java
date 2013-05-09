package com.thinkaurelius.titan.graphdb.database.idhandling;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IDHandler {

    public final static StaticBuffer getKey(long id) {
        assert id >= 0;
        return ByteBufferUtil.getLongBuffer(id << 1);
    }

    public final static long getKeyID(StaticBuffer b) {
        long value = b.getLong(0);
        return value >>> 1;
    }

    public final static void writeInlineEdgeType(WriteBuffer out, long etid, IDManager idManager) {
        VariableLong.writePositive(out, idManager.removeGroupID(etid));
        VariableLong.writePositive(out, idManager.getGroupID(etid));
    }

    public final static long readInlineEdgeType(ReadBuffer in, IDManager idManager) {
        long etidNoGroup = VariableLong.readPositive(in);
        return idManager.addGroupID(etidNoGroup, VariableLong.readPositive(in));
    }


    public final static int edgeTypeLength(long etid, IDManager idManager) {
        long groupbits = idManager.getGroupBits();
        int result;
        if (groupbits <= 6) {
            result = 1;
        } else if (groupbits <= 14) {
            result = 2;
        } else {
            result = 4;
        }
        return result + VariableLong.positiveLength(idManager.removeGroupID(etid));
    }

    private final static void writeEdgeTypeGroup(WriteBuffer out, long group, int dirID, IDManager idManager) {
        assert dirID >= 0 && dirID < 4;
        long groupbits = idManager.getGroupBits();
        if (groupbits <= 6) {
            assert group < (1 << 6);
            byte b = (byte) (group | (dirID << 6));
            out.putByte(b);
        } else if (groupbits <= 14) {
            assert group < (1 << 14);
            short s = (short) (group | (dirID << 14));
            out.putShort(s);
        } else {
            Preconditions.checkArgument(groupbits <= 30);
            assert group < (1 << 30);
            int i = (int) (group | (dirID << 30));
            out.putInt(i);
        }
    }

    public final static void writeEdgeType(WriteBuffer out, long etid, int dirID, IDManager idManager) {
        long group = idManager.getGroupID(etid);
        writeEdgeTypeGroup(out, group, dirID, idManager);
        long etidNoGroup = idManager.removeGroupID(etid);
        assert etidNoGroup >= 0;
        VariableLong.writePositive(out, etidNoGroup);
    }

    private static final byte BYTE_GROUPMASK = (1 << 6) - 1;
    private static final short SHORT_GROUPMASK = (1 << 14) - 1;
    private static final int INT_GROUPMASK = (1 << 30) - 1;

    public final static int getDirectionID(byte b) {
        int dirid = b;
        if (dirid < 0) dirid += 256;
        return dirid >> 6;
    }

    public final static long readEdgeType(ReadBuffer in, IDManager idManager) {
        long groupbits = idManager.getGroupBits();
        int group;
        if (groupbits <= 6) {
            group = in.getByte() & BYTE_GROUPMASK;
        } else if (groupbits <= 14) {
            group = in.getShort() & SHORT_GROUPMASK;
        } else {
            group = in.getInt() & INT_GROUPMASK;
        }
        assert group < idManager.getMaxGroupID();
        long etidNoGroup = VariableLong.readPositive(in);
        return idManager.addGroupID(etidNoGroup, group);
    }

    public final static StaticBuffer getEdgeTypeGroup(long groupid, int dirID, IDManager idManager) {
        int len = 4;
        long groupbits = idManager.getGroupBits();
        if (groupbits <= 6) len = 1;
        else if (groupbits <= 14) len = 2;
        WriteBuffer result = new WriteByteBuffer(len);
        writeEdgeTypeGroup(result, groupid, dirID, idManager);
        return result.getStaticBuffer();
    }

    public final static StaticBuffer getEdgeType(long etid, int dirID, IDManager idManager) {
        WriteBuffer b = new WriteByteBuffer(edgeTypeLength(etid, idManager));
        IDHandler.writeEdgeType(b, etid, dirID, idManager);
        return b.getStaticBuffer();
    }

}
