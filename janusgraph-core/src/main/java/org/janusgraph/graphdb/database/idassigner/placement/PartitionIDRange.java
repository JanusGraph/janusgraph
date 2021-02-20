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

package org.janusgraph.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * An instance of this class describes a range of partition ids. This range if defined by a lower id (inclusive)
 * and upper id (exclusive). When lowerId &lt; upperId this partition range is called a proper range since it describes the
 * contiguous block of ids from lowerId until upperId. When lowerId &gt;= upperID then the partition block "wraps around" the
 * specified idUpperBound. In other words, it describes the ids from [lowerId,idUpperBound) AND [0,upperId).
 *
 * It is always true that lowerID and upperID are smaller or equal than idUpperBound.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PartitionIDRange {


    private static final Logger log =
            LoggerFactory.getLogger(PartitionIDRange.class);

    private static final Random random = new Random();

    private final int lowerID;
    private final int upperID;
    private final int idUpperBound;


    public PartitionIDRange(int lowerID, int upperID, int idUpperBound) {
        Preconditions.checkArgument(idUpperBound > 0, "Partition limit %d must be positive", idUpperBound);
        Preconditions.checkArgument(lowerID >= 0, "Negative partition lower bound %d", lowerID);
        Preconditions.checkArgument(lowerID < idUpperBound, "Partition lower bound %d exceeds limit %d", lowerID, idUpperBound);
        Preconditions.checkArgument(upperID >= 0, "Negative partition upper bound %d", upperID);
        Preconditions.checkArgument(upperID <= idUpperBound, "Partition upper bound %d exceeds limit %d", upperID, idUpperBound);
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

    public int[] getAllContainedIDs() {
        int[] result;
        if (lowerID < upperID) { //"Proper" id range
            result = new int[upperID-lowerID];
            int pos=0;
            for (int id=lowerID;id<upperID;id++) {
                result[pos++]=id;
            }
        } else { //Id range "wraps around"
            result = new int[(idUpperBound-lowerID)+(upperID)];
            int pos=0;
            for (int id=0;id<upperID;id++) {
                result[pos++]=id;
            }
            for (int id=lowerID;id<idUpperBound;id++) {
                result[pos++]=id;
            }
        }
        return result;
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

    @Override
    public String toString() {
        return "["+lowerID+","+upperID+")%"+idUpperBound;
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

    /*
    =========== Helper methods to generate PartitionIDRanges ============
     */

    public static List<PartitionIDRange> getGlobalRange(final int partitionBits) {
        Preconditions.checkArgument(partitionBits>=0 && partitionBits<(Integer.SIZE-1),"Invalid partition bits: %s",partitionBits);
        final int partitionIdBound = (1 << (partitionBits));
        return Collections.singletonList(new PartitionIDRange(0, partitionIdBound, partitionIdBound));
    }

    public static List<PartitionIDRange> getIDRanges(final int partitionBits, final List<KeyRange> locals) {
        Preconditions.checkArgument(partitionBits>0 && partitionBits<(Integer.SIZE-1));
        Preconditions.checkArgument(locals!=null && !locals.isEmpty(),"KeyRanges are empty");
        final int partitionIdBound = (1 << (partitionBits));
        final int backShift = Integer.SIZE-partitionBits;
        List<PartitionIDRange> partitionRanges = new ArrayList<>();
        for (KeyRange local : locals) {
            Preconditions.checkArgument(local.getStart().length() >= 4);
            Preconditions.checkArgument(local.getEnd().length() >= 4);
            if (local.getStart().equals(local.getEnd())) { //Start=End => Partition spans entire range
                partitionRanges.add(new PartitionIDRange(0, partitionIdBound, partitionIdBound));
                continue;
            }

            int startInt = local.getStart().getInt(0);
            int lowerID = startInt >>> backShift;
            assert lowerID>=0 && lowerID<partitionIdBound;
            //Lower id must be inclusive, so check that we did not truncate anything!
            boolean truncatedBits = (lowerID<<backShift)!=startInt;
            StaticBuffer start = local.getAt(0);
            for (int i=4;i<start.length() && !truncatedBits;i++) {
                if (start.getByte(i)!=0) truncatedBits=true;
            }
            if (truncatedBits) lowerID+=1; //adjust to make sure we are inclusive
            int upperID = local.getEnd().getInt(0) >>> backShift; //upper id is exclusive
            //Check that we haven't jumped order indicating that the interval was too small
            if ((local.getStart().compareTo(local.getEnd())<0 && lowerID>=upperID)) {
                discardRange(local);
                continue;
            }
            lowerID = lowerID%partitionIdBound; //ensure that lowerID remains within range
            if (lowerID==upperID) { //After re-normalizing, check for interval collision
                discardRange(local);
                continue;
            }
            partitionRanges.add(new PartitionIDRange(lowerID, upperID, partitionIdBound));
        }
        return partitionRanges;
    }


    private static void discardRange(KeyRange local) {
        log.warn("Individual key range is too small for partition block - result would be empty; hence ignored: {}",local);
    }

}
