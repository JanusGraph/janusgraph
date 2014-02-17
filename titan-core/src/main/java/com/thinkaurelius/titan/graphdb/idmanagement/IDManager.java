package com.thinkaurelius.titan.graphdb.idmanagement;


import com.google.common.base.Preconditions;

/**
 * Handles the allocation of ids based on the type of element
 * Responsible for the bit-wise pattern of Titan's internal id scheme.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IDManager {

    /**
     *bit mask- Description (+ indicates defined type, * indicates proper & defined type)
     *
     *      0 - * Normal (user created) Vertex
     *      1 - + Hidden
     *     11 -     * Hidden (user created/triggered) Vertex [for later]
     *     01 -     Schema related vertices
     *    101 -         + Schema Type vertices
     *   0101 -             + Relation Type vertices
     *  00101 -                 * Property Key
     *  10101 -                 * Edge Label
     *   1101 -             Other Type vertices
     * 001101 -                 * Index Type Vertices
     * 011101 -                 * Type Modifier Vertices
     * 101101 -                 ?
     * 111101 -                 ?
     *    001 -         Reserved namespace for other schema stuff
     *
     *
     */
    public enum VertexIDType {
        Vertex {
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
                return true;
            }
        },

        Hidden {
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
        HiddenVertex {
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
                return true;
            }
        },

        IndexDefinition {
            @Override
            final long offset() {
                return 6l;
            }

            @Override
            final long suffix() {
                return 13l;
            }    // 001101b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        TypeModifier {
            @Override
            public final long offset() {
                return 6l;
            }

            @Override
            final long suffix() {
                return 29l;
            } // 011101b

            @Override
            final boolean isProper() {
                return true;
            }
        };

        abstract long offset();

        abstract long suffix();

        abstract boolean isProper();

        public final long addPadding(long count) {
            return (count << offset()) | suffix();
        }

        public final long removePadding(long id) {
            return id >>> offset();
        }

        public final boolean is(long id) {
            return (id & ((1l << offset()) - 1)) == suffix();
        }
    }

    /**
     * Number of bits that need to be reserved from the type ids for storing additional information during serialization
     */
    public static final int TYPE_LEN_RESERVE = 2;

    /**
     * Total number of bits available to a Titan assigned id
     * We use only 63 bits to make sure that all ids are positive
     *
     * @see com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler#getKey(long)
     */
    private static final long TOTAL_BITS = 63;

    /**
     * Maximum number of bits that can be used for the partition prefix of an id
     */
    private static final long MAX_PARTITION_BITS = 30;
    /**
     * Default number of bits used for the partition prefix. 0 means there is no partition prefix
     */
    private static final long DEFAULT_PARTITION_BITS = 0;

    @SuppressWarnings("unused")
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
        assert VertexIDType.Vertex.offset()>0;
        vertexCountBound = (1l << (TOTAL_BITS - partitionBits - VertexIDType.Vertex.offset()));

        partitionOffset = TOTAL_BITS - partitionBits;
    }

    public IDManager() {
        this(DEFAULT_PARTITION_BITS);
    }

    private static long prefixWithOffset(long id, long prefixid, long prefixOffset, long partitionIDBound) {
        assert partitionIDBound >= 0 && prefixOffset < 64;
        if (id < 0) throw new IllegalArgumentException("ID cannot be negative: " + id);
        if (prefixid < 0) throw new IllegalArgumentException("Prefix ID cannot be negative: " + prefixid);
        if (prefixid == 0) return id;
        Preconditions.checkArgument(prefixid<partitionIDBound,"Prefix ID exceeds limit of: %s",partitionIDBound);
        assert id < (1l << prefixOffset) : "ID is too large for prefix offset: " + id + " ( " + prefixOffset + " )";
        return (prefixid << prefixOffset) | id;
    }


    private long addPartition(long id, long partitionID) {
        assert id > 0;
        assert partitionID >= 0;
        return prefixWithOffset(id, partitionID, partitionOffset, partitionIDBound);
    }

    /*		--- TitanElement id bit format ---
      *  [ 0 | partitionID | count | ID padding ]
     */


    public long getRelationID(long count, long partition) {
        Preconditions.checkArgument(count>0 && count< relationCountBound,"Invalid count for bound: %s", relationCountBound);
        return addPartition(count, partition);
    }


    public long getVertexID(long count, long partition) {
        Preconditions.checkArgument(count>0 && count<vertexCountBound,"Invalid count for bound: %s", vertexCountBound);
        return addPartition(VertexIDType.Vertex.addPadding(count), partition);
    }


    /* --- TitanRelation Type id bit format ---
      *  [ 0 | count | ID padding ]
     */

    private static long getSchemaIdBound(VertexIDType type) {
        assert VertexIDType.SchemaType.is(type.suffix()) : "Expected schema type but got: " + type;
        assert TYPE_LEN_RESERVE>0;
        return (1l << (TOTAL_BITS - type.offset() - TYPE_LEN_RESERVE));
    }

    private static void checkSchemaTypeId(VertexIDType type, long count) {
        Preconditions.checkArgument(VertexIDType.SchemaType.is(type.suffix()),"Expected schema type but got: %s",type);
        Preconditions.checkArgument(type.isProper(),"Expected proper type but got: %s",type);
        long idBound = getSchemaIdBound(type);
        Preconditions.checkArgument(count > 0 && count < idBound,
                "Invalid id [%s] for type [%s] bound: %s", count, type, idBound);
    }

    public static long getSchemaId(VertexIDType type, long count) {
        checkSchemaTypeId(type,count);
        return type.addPadding(count);
    }

    public static long getSchemaIdCount(VertexIDType type, long id) {
        Preconditions.checkArgument(type.is(id));
        return type.removePadding(id);
    }

    public static long getRelationTypeIdCount(long id) {
        Preconditions.checkArgument(VertexIDType.RelationType.is(id));
        return VertexIDType.EdgeLabel.removePadding(id);
    }

    public long getRelationCountBound() {
        return relationCountBound;
    }

    public long getRelationTypeCountBound() {
        return getSchemaIdBound(VertexIDType.EdgeLabel);
    }

    public long getVertexCountBound() {
        return vertexCountBound;
    }

    public long getPartitionBound() {
        return partitionIDBound;
    }


    public long getPartitionId(long id) {
        Preconditions.checkArgument(!VertexIDType.SchemaType.is(id), "Types don't have a partition: %s", id);
        return (id >>> partitionOffset);
    }

    public long isolatePartitionId(long id) {
        return getPartitionId(id) << partitionOffset;
    }

    private final IDInspector inspector = new IDInspector() {

        @Override
        public final boolean isRelationTypeId(long id) {
            return VertexIDType.RelationType.is(id);
        }

        @Override
        public final boolean isEdgeLabelId(long id) {
            return VertexIDType.EdgeLabel.is(id);
        }

        @Override
        public final boolean isPropertyKeyId(long id) {
            return VertexIDType.PropertyKey.is(id);
        }

        @Override
        public final boolean isVertexId(long id) {
            return VertexIDType.Vertex.is(id);
        }

        @Override
        public final long getPartitionId(long id) {
            return IDManager.this.getPartitionId(id);
        }
    };

    public IDInspector getIdInspector() {
        return inspector;
    }

}
