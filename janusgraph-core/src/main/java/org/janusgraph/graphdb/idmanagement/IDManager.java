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
     *bit mask- Description (+ indicates defined type, * indicates proper & defined type)
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
                return 1l;
            }

            @Override
            final long suffix() {
                return 0l;
            } // 0b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        NormalVertex {
            @Override
            final long offset() {
                return 3l;
            }

            @Override
            final long suffix() {
                return 0l;
            } // 000b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        PartitionedVertex {
            @Override
            final long offset() {
                return 3l;
            }

            @Override
            final long suffix() {
                return 2l;
            } // 010b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        UnmodifiableVertex {
            @Override
            final long offset() {
                return 3l;
            }

            @Override
            final long suffix() {
                return 4l;
            } // 100b

            @Override
            final boolean isProper() {
                return true;
            }
        },

        Invisible {
            @Override
            final long offset() {
                return 1l;
            }

            @Override
            final long suffix() {
                return 1l;
            } // 1b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        InvisibleVertex {
            @Override
            final long offset() {
                return 2l;
            }

            @Override
            final long suffix() {
                return 3l;
            } // 11b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        Schema {
            @Override
            final long offset() {
                return 2l;
            }

            @Override
            final long suffix() {
                return 1l;
            } // 01b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        SchemaType {
            @Override
            final long offset() {
                return 3l;
            }

            @Override
            final long suffix() {
                return 5l;
            } // 101b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        RelationType {
            @Override
            final long offset() {
                return 4l;
            }

            @Override
            final long suffix() {
                return 5l;
            } // 0101b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        PropertyKey {
            @Override
            final long offset() {
                return 5l;
            }

            @Override
            final long suffix() {
                return 5l;
            }    // 00101b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        UserPropertyKey {
            @Override
            final long offset() {
                return 6l;
            }

            @Override
            final long suffix() {
                return 5l;
            }    // 000101b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        SystemPropertyKey {
            @Override
            final long offset() {
                return 6l;
            }

            @Override
            final long suffix() {
                return 37l;
            }    // 100101b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        EdgeLabel {
            @Override
            final long offset() {
                return 5l;
            }

            @Override
            final long suffix() {
                return 21l;
            } // 10101b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        UserEdgeLabel {
            @Override
            final long offset() {
                return 6l;
            }

            @Override
            final long suffix() {
                return 21l;
            } // 010101b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        SystemEdgeLabel {
            @Override
            final long offset() {
                return 6l;
            }

            @Override
            final long suffix() {
                return 53l;
            } // 110101b

            @Override
            final boolean isProper() {
                return true;
            }
        },

        VertexLabel {
            @Override
            final long offset() {
                return 5l;
            }

            @Override
            final long suffix() {
                return 13l;
            }    // 01101b

            @Override
            final boolean isProper() {
                return true;
            }
        },

        GenericSchemaType {
            @Override
            final long offset() {
                return 4l;
            }

            @Override
            final long suffix() {
                return 9l;
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
            Preconditions.checkArgument(count>0 && count<(1l<<(TOTAL_BITS-offset())),"Count out of range for type [%s]: %s",this,count);
            return (count << offset()) | suffix();
        }

        public final long removePadding(long id) {
            return id >>> offset();
        }

        public final boolean is(long id) {
            return (id & ((1l << offset()) - 1)) == suffix();
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
    private static final long SCHEMA_COUNT_BOUND = (1l << (TOTAL_BITS - MAX_PADDING_BITWIDTH - TYPE_LEN_RESERVE));


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

        partitionIDBound = (1l << (partitionBits));

        relationCountBound = partitionBits==0?Long.MAX_VALUE:(1l << (TOTAL_BITS - partitionBits));
        assert VertexIDType.NormalVertex.offset()>0;
        vertexCountBound = (1l << (TOTAL_BITS - partitionBits - USERVERTEX_PADDING_BITWIDTH));


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

    private static VertexIDType getUserVertexIDType(long vertexid) {
        VertexIDType type=null;
        if (VertexIDType.NormalVertex.is(vertexid)) type=VertexIDType.NormalVertex;
        else if (VertexIDType.PartitionedVertex.is(vertexid)) type=VertexIDType.PartitionedVertex;
        else if (VertexIDType.UnmodifiableVertex.is(vertexid)) type=VertexIDType.UnmodifiableVertex;
        if (null == type) {
            throw new InvalidIDException("Vertex ID " + vertexid + " has unrecognized type");
        }
        return type;
    }

    public final boolean isUserVertexId(long vertexid) {
        return (VertexIDType.NormalVertex.is(vertexid) || VertexIDType.PartitionedVertex.is(vertexid) || VertexIDType.UnmodifiableVertex.is(vertexid))
                && ((vertexid>>>(partitionBits+USERVERTEX_PADDING_BITWIDTH))>0);
    }

    public long getPartitionId(long vertexid) {
        if (VertexIDType.Schema.is(vertexid)) return SCHEMA_PARTITION;
        assert isUserVertexId(vertexid) && getUserVertexIDType(vertexid)!=null;
        long partition = (vertexid>>>USERVERTEX_PADDING_BITWIDTH) & (partitionIDBound-1);
        assert partition>=0;
        return partition;
    }

    public StaticBuffer getKey(long vertexid) {
        if (VertexIDType.Schema.is(vertexid)) {
            //No partition for schema vertices
            return BufferUtil.getLongBuffer(vertexid);
        } else {
            assert isUserVertexId(vertexid);
            VertexIDType type = getUserVertexIDType(vertexid);
            assert type.offset()==USERVERTEX_PADDING_BITWIDTH;
            long partition = getPartitionId(vertexid);
            long count = vertexid>>>(partitionBits+USERVERTEX_PADDING_BITWIDTH);
            assert count>0;
            long keyid = (partition<<partitionOffset) | type.addPadding(count);
            return BufferUtil.getLongBuffer(keyid);
        }
    }

    public long getKeyID(StaticBuffer b) {
        long value = b.getLong(0);
        if (VertexIDType.Schema.is(value)) {
            return value;
        } else {
            VertexIDType type = getUserVertexIDType(value);
            long partition = partitionOffset<Long.SIZE?value>>>partitionOffset:0;
            long count = (value>>>USERVERTEX_PADDING_BITWIDTH) & ((1l<<(partitionOffset-USERVERTEX_PADDING_BITWIDTH))-1);
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
        return (1l<<63) | id; //make negative but preserve bit pattern
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
        long typeid = VertexIDType.RelationType.addPadding(id);
        Preconditions.checkArgument(isProperRelationType(typeid));
        return typeid;
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

//    public boolean isPartitionedVertex(long id) {
//        return IDManager.this.isPartitionedVertex(id);
//    }
//
//    public long getCanonicalVertexId(long partitionedVertexId) {
//        return IDManager.this.getCanonicalVertexId(partitionedVertexId);
//    }

//    /* ########################################################
//               Inspector
//   ########################################################  */
//
//
//    private final IDInspector inspector = new IDInspector() {
//
//        @Override
//        public final boolean isSchemaVertexId(long id) {
//            return isRelationTypeId(id) || isVertexLabelVertexId(id) || isGenericSchemaVertexId(id);
//        }
//
//        @Override
//        public final boolean isRelationTypeId(long id) {
//            return VertexIDType.RelationType.is(id);
//        }
//
//        @Override
//        public final boolean isEdgeLabelId(long id) {
//            return VertexIDType.EdgeLabel.is(id);
//        }
//
//        @Override
//        public final boolean isPropertyKeyId(long id) {
//            return VertexIDType.PropertyKey.is(id);
//        }
//
//        @Override
//        public boolean isSystemRelationTypeId(long id) {
//            return IDManager.isSystemRelationTypeId(id);
//        }
//
//        @Override
//        public boolean isGenericSchemaVertexId(long id) {
//            return VertexIDType.GenericSchemaType.is(id);
//        }
//
//        @Override
//        public boolean isVertexLabelVertexId(long id) {
//            return VertexIDType.VertexLabel.is(id);
//        }
//
//
//
//        @Override
//        public final boolean isUserVertexId(long id) {
//            return IDManager.this.isUserVertex(id);
//        }
//
//        @Override
//        public boolean isUnmodifiableVertex(long id) {
//            return isUserVertex(id) && VertexIDType.UnmodifiableVertex.is(id);
//        }
//
//        @Override
//        public boolean isPartitionedVertex(long id) {
//            return IDManager.this.isPartitionedVertex(id);
//        }
//
//        @Override
//        public long getCanonicalVertexId(long partitionedVertexId) {
//            return IDManager.this.getCanonicalVertexId(partitionedVertexId);
//        }
//
//    };
//
//    public IDInspector getIdInspector() {
//        return inspector;
//    }

}
