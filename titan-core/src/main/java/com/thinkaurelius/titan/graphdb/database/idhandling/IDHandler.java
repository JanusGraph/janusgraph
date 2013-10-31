package com.thinkaurelius.titan.graphdb.database.idhandling;

import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.RelationType;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IDHandler {

    public static final StaticBuffer MIN_KEY = ByteBufferUtil.getLongBuffer(0);
    public static final StaticBuffer MAX_KEY = ByteBufferUtil.getLongBuffer(-1);


    public static StaticBuffer getKey(long id) {
        assert id >= 0;
        return ByteBufferUtil.getLongBuffer(id << 1);
    }

    public static long getKeyID(StaticBuffer b) {
        long value = b.getLong(0);
        return value >>> 1;
    }

    //Would only need 1 bit to store relation-type-id, but using two so we can upper bound
    private static final int PREFIX_BIT_LEN = 2;

    public static final int PROPERTY_DIR = 0; //00b
    public static final int EDGE_OUT_DIR = 2; //10b
    public static final int EDGE_IN_DIR = 3;  //11b

    private static int getDirection(int dirID) {
        //0=out, 1=in
        return dirID & 1;
    }

    private static int getRelationType(int dirID) {
        //0=property, 1=edge
        return dirID >>> 1;
    }

    private static int getDirectionID(int relationType, int direction) {
        assert relationType >= 0 && relationType <= 1 && direction >= 0 && direction <= 1;
        return (relationType << 1) + direction;
    }

    public static boolean isValidDirection(final int dirId) {
        return dirId == PROPERTY_DIR || dirId == EDGE_IN_DIR || dirId == EDGE_OUT_DIR;
    }

    public static int edgeTypeLength(long etid) {
        assert etid > 0 && (etid << 1) > 0;  //Check positive and no-overflow
        return VariableLong.positiveWithPrefixLength(IDManager.getTypeCount(etid) << 1, PREFIX_BIT_LEN);
    }


    /**
     * The edge type is written as follows: [ Relation-Type-ID (2 bit) | Relation-Type-Count (variable) | Direction-ID (1 bit)]
     *
     * @param out
     * @param etid
     * @param dirID
     */
    public static void writeEdgeType(WriteBuffer out, long etid, int dirID) {
        assert etid > 0 && (etid << 1) > 0; //Check positive and no-overflow
        assert isValidDirection(dirID);
        etid = (IDManager.getTypeCount(etid) << 1) + getDirection(dirID);
        VariableLong.writePositiveWithPrefix(out, etid, getRelationType(dirID), PREFIX_BIT_LEN);
    }

    public static StaticBuffer getEdgeType(long etid, int dirID) {
        WriteBuffer b = new WriteByteBuffer(edgeTypeLength(etid));
        IDHandler.writeEdgeType(b, etid, dirID);
        return b.getStaticBuffer();
    }

    public static long[] readEdgeType(ReadBuffer in) {
        long[] countPrefix = VariableLong.readPositiveWithPrefix(in, PREFIX_BIT_LEN);
        int dirID = getDirectionID((int) countPrefix[1], (int) (countPrefix[0] & 1));
        countPrefix[1] = dirID;
        countPrefix[0] = countPrefix[0] >>> 1;

        if (countPrefix[1] == PROPERTY_DIR)
            countPrefix[0] = IDManager.getPropertyKeyID(countPrefix[0]);
        else if (countPrefix[1] == EDGE_IN_DIR || countPrefix[1] == EDGE_OUT_DIR)
            countPrefix[0] = IDManager.getEdgeLabelID(countPrefix[0]);
        else
            throw new AssertionError("Invalid direction ID: " + countPrefix[1]);
        return countPrefix;
    }


    public static void writeInlineEdgeType(WriteBuffer out, long etid) {
        long compressId = IDManager.getTypeCount(etid) << 1;
        if (IDManager.isPropertyKeyID(etid))
            compressId += 0;
        else if (IDManager.isEdgeLabelID(etid))
            compressId += 1;
        else throw new AssertionError("Invalid type id: " + etid);
        VariableLong.writePositive(out, compressId);
    }

    public static long readInlineEdgeType(ReadBuffer in) {
        long compressId = VariableLong.readPositive(in);
        long id = compressId >>> 1;
        switch ((int) (compressId & 1)) {
            case 0:
                id = IDManager.getPropertyKeyID(id);
                break;
            case 1:
                id = IDManager.getEdgeLabelID(id);
                break;
            default:
                throw new AssertionError("Invalid type: " + compressId);
        }
        return id;
    }

    private static StaticBuffer getPrefixed(int prefix) {
        assert prefix < (1 << PREFIX_BIT_LEN) && prefix >= 0;
        byte[] arr = new byte[1];
        arr[0] = (byte) (prefix << (Byte.SIZE - PREFIX_BIT_LEN));
        return new StaticArrayBuffer(arr);
    }

    public static StaticBuffer[] getBounds(RelationType type) {
        int start, end;
        switch (type) {
            case PROPERTY:
                start = getRelationType(PROPERTY_DIR);
                end = start + 1;
                break;
            case EDGE:
                start = getRelationType(EDGE_OUT_DIR);
                end = start + 1;
                break;
            case RELATION:
                start = getRelationType(PROPERTY_DIR);
                end = getRelationType(EDGE_OUT_DIR) + 1;
                break;
            default:
                throw new AssertionError("Unrecognized type:" + type);
        }
        assert end > start;
        return new StaticBuffer[]{getPrefixed(start), getPrefixed(end)};
    }

}
