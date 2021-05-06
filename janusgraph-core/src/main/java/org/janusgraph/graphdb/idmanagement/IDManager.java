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

package org.janusgraph.graphdb.idmanagement;


import com.google.common.base.Preconditions;
import org.janusgraph.core.InvalidIDException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.graphdb.database.idhandling.VariableLong;

/**
 * Handles the allocation of ids based on the type of element
 * Responsible for the bit-wise pattern of JanusGraph's internal id scheme.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IDManager {

    /**
     *bit mask- Description (+ indicates defined type, * indicates proper &amp; defined type)
     *
     *      0 - + User created Vertex
     *    000 -     * Normal vertices
     *    010 -     * Partitioned vertices
     *    100 -     * Unmodifiable (e.g. TTL'ed) vertices
     *    110 -     + Reserved for additional vertex type
     *      1 - + Invisible
     *     11 -     * Invisible (user created/triggered) Vertex [for later]
     *     01 -     + Schema related vertices
     *    101 -         + Schema Type vertices
     *   0101 -             + Relation Type vertices
     *  00101 -                 + Property Key
     * 000101 -                     * User Property Key
     * 100101 -                     * System Property Key
     *  10101 -                 + Edge Label
     * 010101 -                     * User Edge Label
     * 110101 -                     * System Edge Label
     *   1101 -             Other Type vertices
     *  01101 -                 * Vertex Label
     *    001 -         Non-Type vertices
     *   1001 -             * Generic Schema Vertex
     *   0001 -             Reserved for future
     *
     *
     */
    public enum VertexIDType {
        UserVertex {
            @Override
            final long offset() {
                return 1L;
            }

            @Override
            final long suffix() {
                return 0L;
            } // 0b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        NormalVertex {
            @Override
            final long offset() {
                return 3L;
            }

            @Override
            final long suffix() {
                return 0L;
            } // 000b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        PartitionedVertex {
            @Override
            final long offset() {
                return 3L;
            }

            @Override
            final long suffix() {
                return 2L;
            } // 010b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        UnmodifiableVertex {
            @Override
            final long offset() {
                return 3L;
            }

            @Override
            final long suffix() {
                return 4L;
            } // 100b

            @Override
            final boolean isProper() {
                return true;
            }
        },

        Invisible {
            @Override
            final long offset() {
                return 1L;
            }

            @Override
            final long suffix() {
                return 1L;
            } // 1b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        InvisibleVertex {
            @Override
            final long offset() {
                return 2L;
            }

            @Override
            final long suffix() {
                return 3L;
            } // 11b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        Schema {
            @Override
            final long offset() {
                return 2L;
            }

            @Override
            final long suffix() {
                return 1L;
            } // 01b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        SchemaType {
            @Override
            final long offset() {
                return 3L;
            }

            @Override
            final long suffix() {
                return 5L;
            } // 101b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        RelationType {
            @Override
            final long offset() {
                return 4L;
            }

            @Override
            final long suffix() {
                return 5L;
            } // 0101b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        PropertyKey {
            @Override
            final long offset() {
                return 5L;
            }

            @Override
            final long suffix() {
                return 5L;
            }    // 00101b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        UserPropertyKey {
            @Override
            final long offset() {
                return 6L;
            }

            @Override
            final long suffix() {
                return 5L;
            }    // 000101b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        SystemPropertyKey {
            @Override
            final long offset() {
                return 6L;
            }

            @Override
            final long suffix() {
                return 37L;
            }    // 100101b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        EdgeLabel {
            @Override
            final long offset() {
                return 5L;
            }

            @Override
            final long suffix() {
                return 21L;
            } // 10101b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        UserEdgeLabel {
            @Override
            final long offset() {
                return 6L;
            }

            @Override
            final long suffix() {
                return 21L;
            } // 010101b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        SystemEdgeLabel {
            @Override
            final long offset() {
                return 6L;
            }

            @Override
            final long suffix() {
                return 53L;
            } // 110101b

            @Override
            final boolean isProper() {
                return true;
            }
        },

        VertexLabel {
            @Override
            final long offset() {
                return 5L;
            }

            @Override
            final long suffix() {
                return 13L;
            }    // 01101b

            @Override
            final boolean isProper() {
                return true;
            }
        },

        GenericSchemaType {
            @Override
            final long offset() {
                return 4L;
            }

            @Override
            final long suffix() {
                return 9L;
            }    // 1001b

            @Override
            final boolean isProper() {
                return true;
            }
        };

        abstract long offset();

        abstract long suffix();

        abstract boolean isProper();

        public final long addPadding(long count) {
            assert offset()>0;
            Preconditions.checkArgument(count>0 && count<(1L <<(TOTAL_BITS-offset())),"Count out of range for type [%s]: %s",this,count);
            return (count << offset()) | suffix();
        }

        public final long removePadding(long id) {
            return id >>> offset();
        }

        public final boolean is(long id) {
            return (id & ((1L << offset()) - 1)) == suffix();
        }

        public final boolean isSubType(VertexIDType type) {
            return is(type.suffix());
        }
    }

    /**
     * Id of the partition that schema elements are assigned to
     */
    public static final int SCHEMA_PARTITION = 0;

    public static final int PARTITIONED_VERTEX_PARTITION = 1;


    /**
     * Number of bits that need to be reserved from the type ids for storing additional information during serialization
     */
    public static final int TYPE_LEN_RESERVE = 3;

    /**
     * Total number of bits available to a JanusGraph assigned id
     * We use only 63 bits to make sure that all ids are positive
     *
     */
    private static final long TOTAL_BITS = Long.SIZE-1;

    /**
     * Maximum number of bits that can be used for the partition prefix of an id
     */
    private static final long MAX_PARTITION_BITS = 16;
    /**
     * Default number of bits used for the partition prefix. 0 means there is no partition prefix
     */
    private static final long DEFAULT_PARTITION_BITS = 0;
    /**
     * The padding bit width for user vertices
     */
    public static final long USERVERTEX_PADDING_BITWIDTH = VertexIDType.NormalVertex.offset();

    /**
     * The maximum number of padding bits of any type
     */
    public static final long MAX_PADDING_BITWIDTH = VertexIDType.UserEdgeLabel.offset();

    /**
     * Bound on the maximum count for a schema id
     */
    private static final long SCHEMA_COUNT_BOUND = (1L << (TOTAL_BITS - MAX_PADDING_BITWIDTH - TYPE_LEN_RESERVE));


    private final long partitionBits;
    private final long partitionOffset;
    private final long partitionIDBound;

    private final long relationCountBound;
    private final long vertexCountBound;


    public IDManager(long partitionBits) {
        Preconditions.checkArgument(partitionBits >= 0);
        Preconditions.checkArgument(partitionBits <= MAX_PARTITION_BITS,
                "Partition bits can be at most %s bits", MAX_PARTITION_BITS);
        this.partitionBits = partitionBits;

        partitionIDBound = (1L << (partitionBits));

        relationCountBound = partitionBits==0?Long.MAX_VALUE:(1L << (TOTAL_BITS - partitionBits));
        assert VertexIDType.NormalVertex.offset()>0;
        vertexCountBound = (1L << (TOTAL_BITS - partitionBits - USERVERTEX_PADDING_BITWIDTH));


        partitionOffset = Long.SIZE - partitionBits;
    }

    public IDManager() {
        this(DEFAULT_PARTITION_BITS);
    }

    public long getPartitionBound() {
        return partitionIDBound;
    }

    /* ########################################################
                   User Relations and Vertices
       ########################################################  */

     /*		--- JanusGraphElement id bit format ---
      *  [ 0 | count | partition | ID padding (if any) ]
     */

    private long constructId(long count, long partition, VertexIDType type) {
        Preconditions.checkArgument(partition<partitionIDBound && partition>=0,"Invalid partition: %s",partition);
        Preconditions.checkArgument(count>=0);
        Preconditions.checkArgument(VariableLong.unsignedBitLength(count)+partitionBits+
                (type==null?0:type.offset())<=TOTAL_BITS);
        Preconditions.checkArgument(type==null || type.isProper());
        long id = (count<<partitionBits)+partition;
        if (type!=null) id = type.addPadding(id);
        return id;
    }

    private static VertexIDType getUserVertexIDType(long vertexId) {
        VertexIDType type=null;
        if (VertexIDType.NormalVertex.is(vertexId)) type=VertexIDType.NormalVertex;
        else if (VertexIDType.PartitionedVertex.is(vertexId)) type=VertexIDType.PartitionedVertex;
        else if (VertexIDType.UnmodifiableVertex.is(vertexId)) type=VertexIDType.UnmodifiableVertex;
        if (null == type) {
            throw new InvalidIDException("Vertex ID " + vertexId + " has unrecognized type");
        }
        return type;
    }

    public final boolean isUserVertexId(long vertexId) {
        return (VertexIDType.NormalVertex.is(vertexId) || VertexIDType.PartitionedVertex.is(vertexId) || VertexIDType.UnmodifiableVertex.is(vertexId))
                && ((vertexId>>>(partitionBits+USERVERTEX_PADDING_BITWIDTH))>0);
    }

    public long getPartitionId(long vertexId) {
        if (VertexIDType.Schema.is(vertexId)) return SCHEMA_PARTITION;
        assert isUserVertexId(vertexId) && getUserVertexIDType(vertexId)!=null;
        long partition = (vertexId>>>USERVERTEX_PADDING_BITWIDTH) & (partitionIDBound-1);
        assert partition>=0;
        return partition;
    }

    public StaticBuffer getKey(long vertexId) {
        if (VertexIDType.Schema.is(vertexId)) {
            //No partition for schema vertices
            return BufferUtil.getLongBuffer(vertexId);
        } else {
            assert isUserVertexId(vertexId);
            VertexIDType type = getUserVertexIDType(vertexId);
            assert type.offset()==USERVERTEX_PADDING_BITWIDTH;
            long partition = getPartitionId(vertexId);
            long count = vertexId>>>(partitionBits+USERVERTEX_PADDING_BITWIDTH);
            assert count>0;
            long keyId = (partition<<partitionOffset) | type.addPadding(count);
            return BufferUtil.getLongBuffer(keyId);
        }
    }

    public long getKeyID(StaticBuffer b) {
        long value = b.getLong(0);
        if (VertexIDType.Schema.is(value)) {
            return value;
        } else {
            VertexIDType type = getUserVertexIDType(value);
            long partition = partitionOffset<Long.SIZE?value>>>partitionOffset:0;
            long count = (value>>>USERVERTEX_PADDING_BITWIDTH) & ((1L <<(partitionOffset-USERVERTEX_PADDING_BITWIDTH))-1);
            return constructId(count,partition,type);
        }
    }

    public long getRelationID(long count, long partition) {
        Preconditions.checkArgument(count>0 && count< relationCountBound,"Invalid count for bound: %s", relationCountBound);
        return constructId(count, partition, null);
    }


    public long getVertexID(long count, long partition, VertexIDType vertexType) {
        Preconditions.checkArgument(VertexIDType.UserVertex.is(vertexType.suffix()),"Not a user vertex type: %s",vertexType);
        Preconditions.checkArgument(count>0 && count<vertexCountBound,"Invalid count for bound: %s", vertexCountBound);
        if (vertexType==VertexIDType.PartitionedVertex) {
            Preconditions.checkArgument(partition==PARTITIONED_VERTEX_PARTITION);
            return getCanonicalVertexIdFromCount(count);
        } else {
            return constructId(count, partition, vertexType);
        }
    }

    public long getPartitionHashForId(long id) {
        Preconditions.checkArgument(id>0);
        Preconditions.checkState(partitionBits>0, "no partition bits");
        long result = 0;
        int offset = 0;
        while (offset<Long.SIZE) {
            result = result ^ ((id>>>offset) & (partitionIDBound-1));
            offset+=partitionBits;
        }
        assert result>=0 && result<partitionIDBound;
        return result;
    }

    private long getCanonicalVertexIdFromCount(long count) {
        long partition = getPartitionHashForId(count);
        return constructId(count,partition,VertexIDType.PartitionedVertex);
    }

    public long getCanonicalVertexId(long partitionedVertexId) {
        Preconditions.checkArgument(VertexIDType.PartitionedVertex.is(partitionedVertexId));
        long count = partitionedVertexId>>>(partitionBits+USERVERTEX_PADDING_BITWIDTH);
        return getCanonicalVertexIdFromCount(count);
    }

    public boolean isCanonicalVertexId(long partitionVertexId) {
        return partitionVertexId==getCanonicalVertexId(partitionVertexId);
    }

    public long getPartitionedVertexId(long partitionedVertexId, long otherPartition) {
        Preconditions.checkArgument(VertexIDType.PartitionedVertex.is(partitionedVertexId));
        long count = partitionedVertexId>>>(partitionBits+USERVERTEX_PADDING_BITWIDTH);
        assert count>0;
        return constructId(count,otherPartition,VertexIDType.PartitionedVertex);
    }

    public long[] getPartitionedVertexRepresentatives(long partitionedVertexId) {
        Preconditions.checkArgument(isPartitionedVertex(partitionedVertexId));
        assert getPartitionBound()<Integer.MAX_VALUE;
        long[] ids = new long[(int)getPartitionBound()];
        for (int i=0;i<getPartitionBound();i++) {
            ids[i]=getPartitionedVertexId(partitionedVertexId,i);
        }
        return ids;
    }

    /**
     * Converts a user provided long id into a JanusGraph vertex id. The id must be positive and less than {@link #getVertexCountBound()}.
     * This method is useful when providing ids during vertex creation via {@link org.apache.tinkerpop.gremlin.structure.Graph#addVertex(Object...)}.
     *
     * @param id long id
     * @return a corresponding JanusGraph vertex id
     * @see #fromVertexId(long)
     */
    public long toVertexId(long id) {
        Preconditions.checkArgument(id > 0, "Vertex id must be positive: %s", id);
        Preconditions.checkArgument(vertexCountBound > id, "Vertex id is too large: %s", id);
        return id<<(partitionBits+USERVERTEX_PADDING_BITWIDTH);
    }

    /**
     * Converts a JanusGraph vertex id to the user provided id as the inverse mapping of {@link #toVertexId(long)}.
     *
     * @param id JanusGraph vertex id (must be positive)
     * @return original user provided id
     * @see #toVertexId(long)
     */
    public long fromVertexId(long id) {
        Preconditions.checkArgument(id >>> USERVERTEX_PADDING_BITWIDTH+partitionBits > 0
            && id <= (vertexCountBound-1)<<USERVERTEX_PADDING_BITWIDTH+partitionBits, "Invalid vertex id provided: %s", id);
        return id>>USERVERTEX_PADDING_BITWIDTH+partitionBits;
    }

    public boolean isPartitionedVertex(long id) {
        return isUserVertexId(id) && VertexIDType.PartitionedVertex.is(id);
    }

    public long getRelationCountBound() {
        return relationCountBound;
    }

    public long getVertexCountBound() {
        return vertexCountBound;
    }

    /*

    Temporary ids are negative and don't have partitions

     */

    public static long getTemporaryRelationID(long count) {
        return makeTemporary(count);
    }

    public static long getTemporaryVertexID(VertexIDType type, long count) {
        Preconditions.checkArgument(type.isProper(),"Invalid vertex id type: %s",type);
        return makeTemporary(type.addPadding(count));
    }

    private static long makeTemporary(long id) {
        Preconditions.checkArgument(id>0);
        return (1L <<63) | id; //make negative but preserve bit pattern
    }

    public static boolean isTemporary(long id) {
        return id<0;
    }

    /* ########################################################
               Schema Vertices
   ########################################################  */

    /* --- JanusGraphRelation Type id bit format ---
      *  [ 0 | count | ID padding ]
      *  (there is no partition)
     */


    private static void checkSchemaTypeId(VertexIDType type, long count) {
        Preconditions.checkArgument(VertexIDType.Schema.is(type.suffix()),"Expected schema vertex but got: %s",type);
        Preconditions.checkArgument(type.isProper(),"Expected proper type but got: %s",type);
        Preconditions.checkArgument(count > 0 && count < SCHEMA_COUNT_BOUND,
                "Invalid id [%s] for type [%s] bound: %s", count, type, SCHEMA_COUNT_BOUND);
    }

    public static long getSchemaId(VertexIDType type, long count) {
        checkSchemaTypeId(type,count);
        return type.addPadding(count);
    }

    private static boolean isProperRelationType(long id) {
        return VertexIDType.UserEdgeLabel.is(id) || VertexIDType.SystemEdgeLabel.is(id)
                || VertexIDType.UserPropertyKey.is(id) || VertexIDType.SystemPropertyKey.is(id);
    }

    public static long stripEntireRelationTypePadding(long id) {
        Preconditions.checkArgument(isProperRelationType(id));
        return VertexIDType.UserEdgeLabel.removePadding(id);
    }

    public static long stripRelationTypePadding(long id) {
        Preconditions.checkArgument(isProperRelationType(id));
        return VertexIDType.RelationType.removePadding(id);
    }

    public static long addRelationTypePadding(long id) {
        long typeId = VertexIDType.RelationType.addPadding(id);
        Preconditions.checkArgument(isProperRelationType(typeId));
        return typeId;
    }

    public static boolean isSystemRelationTypeId(long id) {
        return VertexIDType.SystemEdgeLabel.is(id) || VertexIDType.SystemPropertyKey.is(id);
    }

    public static long getSchemaCountBound() {
        return SCHEMA_COUNT_BOUND;
    }

    //ID inspection ------------------------------

    public final boolean isSchemaVertexId(long id) {
        return isRelationTypeId(id) || isVertexLabelVertexId(id) || isGenericSchemaVertexId(id);
    }

    public final boolean isRelationTypeId(long id) {
        return VertexIDType.RelationType.is(id);
    }

    public final boolean isEdgeLabelId(long id) {
        return VertexIDType.EdgeLabel.is(id);
    }

    public final boolean isPropertyKeyId(long id) {
        return VertexIDType.PropertyKey.is(id);
    }

    public boolean isGenericSchemaVertexId(long id) {
        return VertexIDType.GenericSchemaType.is(id);
    }

    public boolean isVertexLabelVertexId(long id) {
        return VertexIDType.VertexLabel.is(id);
    }

    public boolean isUnmodifiableVertex(long id) {
        return isUserVertexId(id) && VertexIDType.UnmodifiableVertex.is(id);
    }
}
