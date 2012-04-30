package com.thinkaurelius.titan.graphdb.database.idhandling;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class IDHandler {
    
    public final static ByteBuffer getKey(long id) {
        assert id>=0;
        return ByteBufferUtil.getLongByteBuffer(id<<1);
    }
    
    public final static long getKeyID(ByteBuffer b) {
        long value = b.getLong();
        return value>>>1;
    }
    
    public final static void writeInlineEdgeType(DataOutput out, long etid, IDManager idManager) {
        VariableLong.writePositive(out,idManager.removeGroupID(etid));
        VariableLong.writePositive(out,idManager.getGroupID(etid));
    }
    
    public final static long readInlineEdgeType(ByteBuffer in, IDManager idManager) {
        long etidNoGroup = VariableLong.readPositive(in);
        return idManager.addGroupID(etidNoGroup,VariableLong.readPositive(in));
    }


    public final static int edgeTypeLength(long etid, IDManager idManager) {
        long groupbits = idManager.getGroupBits();
        int result;
        if (groupbits<=6) {
            result = 1;
        } else if (groupbits<=14) {
            result = 2;
        } else {
            result = 4;
        }
        return result + VariableLong.positiveLength(idManager.removeGroupID(etid));
    }

    private final static void writeEdgeTypeGroup(ByteBuffer out, long group, int dirID, IDManager idManager) {
        assert dirID>=0 && dirID<4;
        long groupbits = idManager.getGroupBits();
        if (groupbits<=6) {
            assert group<(1<<6);
            byte b = (byte)(group | (dirID<<6));
            out.put(b);
        } else if (groupbits<=14) {
            assert group<(1<<14);
            short s = (short)(group | (dirID<<14));
            out.putShort(s);
        } else {
            Preconditions.checkArgument(groupbits<=30);
            assert group<(1<<30);
            int i = (int)(group | (dirID<<30));
            out.putInt(i);
        }

    }
    
    public final static void writeEdgeType(ByteBuffer out, long etid, int dirID, IDManager idManager) {
        long group = idManager.getGroupID(etid);
        writeEdgeTypeGroup(out,group,dirID,idManager);
        long etidNoGroup = idManager.removeGroupID(etid);
        assert etidNoGroup>=0;
        VariableLong.writePositive(out,etidNoGroup);
    }
    
    private static final byte BYTE_GROUPMASK = (1<<6) - 1;
    private static final short SHORT_GROUPMASK = (1<<14) - 1;
    private static final int INT_GROUPMASK = (1<<30) - 1;
    
    public final static int getDirectionID(byte b) {
        int dirid = b;
        if (dirid<0) dirid+=256;
        return dirid>>6;
    }
    
    public final static long readEdgeType(ByteBuffer in, IDManager idManager) {
        long groupbits = idManager.getGroupBits();
        int group;
        if (groupbits<=6) {
            group = in.get() & BYTE_GROUPMASK;
        } else if (groupbits<=14) {
            group = in.getShort() & SHORT_GROUPMASK;
        } else {
            group = in.getInt() & INT_GROUPMASK;
        }
        assert group<idManager.getMaxGroupID();
        long etidNoGroup = VariableLong.readPositive(in);
        return idManager.addGroupID(etidNoGroup,group);
    }
    
    public final static ByteBuffer getEdgeTypeGroup(long groupid, int dirID, IDManager idManager) {
        int len = 4;
        long groupbits = idManager.getGroupBits();
        if (groupbits<=6) len = 1;
        else if (groupbits<=14) len =2;
        ByteBuffer result = ByteBuffer.allocate(len);
        writeEdgeTypeGroup(result, groupid,dirID,idManager);
        result.flip();
        return result;
    }
    
    public final static ByteBuffer getEdgeType(long etid, int dirID, IDManager idManager) {
        ByteBuffer b = ByteBuffer.allocate(edgeTypeLength(etid,idManager));
        IDHandler.writeEdgeType(b,etid,dirID,idManager);
        b.flip();
        return b;
    }

    // =============== THIS IS A COPY&PASTE OF THE ABOVE =================
    // Using DataOutput instead of ByteBuffer

    public final static void writeEdgeType(DataOutput out, long etid, int dirID, IDManager idManager) {
        assert dirID>=0 && dirID<4;
        long groupbits = idManager.getGroupBits();
        long group = idManager.getGroupID(etid);
        if (groupbits<=6) {
            assert group<(1<<6);
            byte b = (byte)(group | (dirID<<6));
            out.putByte(b);
        } else if (groupbits<=14) {
            assert group<(1<<14);
            short s = (short)(group | (dirID<<14));
            out.putShort(s);
        } else {
            Preconditions.checkArgument(groupbits<=30);
            assert group<(1<<30);
            int i = (int)(group | (dirID<<30));
            out.putInt(i);
        }
        long etidNoGroup = idManager.removeGroupID(etid);
        assert etidNoGroup>=0;
        VariableLong.writePositive(out,etidNoGroup);
    }

}
