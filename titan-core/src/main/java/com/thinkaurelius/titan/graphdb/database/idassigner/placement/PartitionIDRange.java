package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;

import java.util.Random;

/**
 * An instance of this class describes a range of partition ids. This range if defined by a lower id (inclusive)
 * and upper id (exclusive). When lowerId < upperId this partition range is called a proper range since it describes the
 * contiguous block of ids from lowerId until upperId. When lowerId >= upperID then the partition block "wraps around" the
 * specified idUpperBound. In other words, it describes the ids from [lowerId,idUpperBound) AND [0,upperId).
 *
 * It is always true that lowerID and upperID are smaller or equal than idUpperBound.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PartitionIDRange {

    private static final Random random = new Random();

    private final int lowerID;
    private final int upperID;
    private final int idUpperBound;


    public PartitionIDRange(int lowerID, int upperID, int idUpperBound) {
        Preconditions.checkArgument(idUpperBound>0, "Partition limit " + idUpperBound + " must be positive");
        Preconditions.checkArgument(idUpperBound<=Integer.MAX_VALUE, "Partition limit cannot exceed representable range of an integer");
        Preconditions.checkArgument(lowerID>=0, "Negative partition lower bound " + lowerID);
        Preconditions.checkArgument(lowerID< idUpperBound, "Partition lower bound " + lowerID + " exceeds limit " + idUpperBound);
        Preconditions.checkArgument(upperID>=0, "Negative partition upper bound " + upperID);
        Preconditions.checkArgument(upperID<=idUpperBound, "Partition upper bound " + upperID + " exceeds limit " + idUpperBound);
        this.lowerID = lowerID;
        this.upperID = upperID;
        this.idUpperBound = idUpperBound;
    }

    public int getLowerID() {
        return lowerID;
    }

    public int getUpperID() {
        return upperID;
    }

    public int getIdUpperBound() {
        return idUpperBound;
    }

    /**
     * Returns true of the given partitionId lies within this partition id range, else false.
     *
     * @param partitionId
     * @return
     */
    public boolean contains(int partitionId) {
        if (lowerID < upperID) { //"Proper" id range
            return lowerID <= partitionId && upperID > partitionId;
        } else { //Id range "wraps around"
            return (lowerID <= partitionId && partitionId < idUpperBound) ||
                    (upperID > partitionId && partitionId >= 0);
        }
    }

    /**
     * Returns a random partition id that lies within this partition id range.
     *
     * @return
     */
    public int getRandomID() {
        //Compute the width of the partition...
        int partitionWidth;
        if (lowerID < upperID) partitionWidth = upperID - lowerID; //... for "proper" ranges
        else partitionWidth = (idUpperBound - lowerID) + upperID; //... and those that "wrap around"
        Preconditions.checkArgument(partitionWidth > 0, partitionWidth);
        return (random.nextInt(partitionWidth) + lowerID) % idUpperBound;
    }



}
