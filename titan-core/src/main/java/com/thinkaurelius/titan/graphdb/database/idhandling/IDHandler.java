package com.thinkaurelius.titan.graphdb.database.idhandling;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;

import java.nio.ByteBuffer;

import static com.thinkaurelius.titan.graphdb.idmanagement.IDManager.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IDHandler {

    public final static StaticBuffer getKey(long id) {
        Preconditions.checkArgument(id >= 0);
        return ByteBufferUtil.getLongBuffer(id << 1);
    }

    public final static long getKeyID(StaticBuffer b) {
        long value = b.getLong(0);
        return value >>> 1;
    }



    public static final boolean isValidDirection(final int dirId) {
        return dirId==PROPERTY_DIR || dirId==EDGE_IN_DIR || dirId==EDGE_OUT_DIR;
    }

    public final static int edgeTypeLength(long etid) {
        return VariableLong.positiveWithPrefixLength(IDManager.getTypeCount(etid), DIR_BIT_LEN);
    }

    public final static void writeEdgeType(WriteBuffer out, long etid, int dirID) {
        Preconditions.checkArgument(isValidDirection(dirID));
        VariableLong.writePositiveWithPrefix(out,IDManager.getTypeCount(etid),dirID, DIR_BIT_LEN);
    }

    public final static StaticBuffer getEdgeType(long etid, int dirID) {
        WriteBuffer b = new WriteByteBuffer(edgeTypeLength(etid));
        IDHandler.writeEdgeType(b, etid, dirID);
        return b.getStaticBuffer();
    }

    public final static long[] readEdgeType(ReadBuffer in) {
        long[] countPrefix = VariableLong.readPositiveWithPrefix(in, DIR_BIT_LEN);
        if (countPrefix[1]==PROPERTY_DIR)
            countPrefix[0] = IDManager.getPropertyKeyID(countPrefix[0]);
        else if (countPrefix[1]==EDGE_IN_DIR || countPrefix[1]==EDGE_OUT_DIR)
            countPrefix[0] = IDManager.getEdgeLabelID(countPrefix[0]);
        else throw new AssertionError("Invalid direction ID: " + countPrefix[1]);
        return countPrefix;
    }


    public final static void writeInlineEdgeType(WriteBuffer out, long etid) {
        long compressId = IDManager.getTypeCount(etid)<<1;
        if (IDManager.isPropertyKeyID(etid))
            compressId += 0;
        else if (IDManager.isEdgeLabelID(etid))
            compressId += 1;
        else throw new AssertionError("Invalid type id: " + etid);
        VariableLong.writePositive(out,compressId);
    }

    public final static long readInlineEdgeType(ReadBuffer in) {
        long compressId = VariableLong.readPositive(in);
        long id = compressId>>>1;
        switch((int)(compressId & 1)) {
            case 0:
                id = IDManager.getPropertyKeyID(id);
                break;
            case 1:
                id = IDManager.getEdgeLabelID(id);
                break;
            default: throw new AssertionError("Invalid type: " + compressId);
        }
        return id;
    }


    public static final StaticBuffer directionPlusZero(int dirId) {
        Preconditions.checkArgument(isValidDirection(dirId));
        byte[] arr = new byte[9];
        arr[0] = (byte)(dirId<<(Byte.SIZE- DIR_BIT_LEN));
        return new StaticArrayBuffer(arr);
    }

    public static final StaticBuffer directionPlusOne(int dirId) {
        Preconditions.checkArgument(isValidDirection(dirId));
        byte[] arr = new byte[9];
        for (int i=0;i<arr.length;i++) arr[i]=(byte)-1;
        arr[0] = (byte)((dirId<<(Byte.SIZE- DIR_BIT_LEN)) + (1<<(Byte.SIZE- DIR_BIT_LEN)) - 1);
        return new StaticArrayBuffer(arr);
    }


}
