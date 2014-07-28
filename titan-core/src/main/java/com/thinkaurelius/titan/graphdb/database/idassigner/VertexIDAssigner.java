package com.thinkaurelius.titan.graphdb.database.idassigner;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.relations.ReassignableRelation;
import com.thinkaurelius.titan.util.stats.NumberUtil;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.IDAuthority;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.*;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanSchemaVertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

@PreInitializeConfigOptions
public class VertexIDAssigner {

    private static final Logger log =
            LoggerFactory.getLogger(VertexIDAssigner.class);

    private static final int MAX_PARTITION_RENEW_ATTEMPTS = 1000;

    public static final ConfigOption<String> PLACEMENT_STRATEGY = new ConfigOption<String>(IDS_NS,"placement",
            "Name of the vertex placement strategy or full class name", ConfigOption.Type.MASKABLE, "simplebulk");

    private static final Map<String,String> REGISTERED_PLACEMENT_STRATEGIES = ImmutableMap.of(
            "simplebulk", SimpleBulkPlacementStrategy.class.getName()
    );


    final ConcurrentMap<Integer,PartitionIDPool> idPools;
    final StandardIDPool schemaIdPool;
    final StandardIDPool partitionVertexIdPool;

    private final IDAuthority idAuthority;
    private final IDManager idManager;
    private final IDPlacementStrategy placementStrategy;

    //For StandardIDPool
    private final Duration renewTimeoutMS;
    private final double renewBufferPercentage;

    private final int partitionIdBound;
    private final boolean hasLocalPartitions;

    public VertexIDAssigner(Configuration config, IDAuthority idAuthority, StoreFeatures idAuthFeatures) {
        Preconditions.checkNotNull(idAuthority);
        this.idAuthority = idAuthority;

        int partitionBits;
        boolean partitionIDs = config.get(CLUSTER_PARTITION);
        if (partitionIDs) {
            //Use a placement strategy that balances partitions
            partitionBits = NumberUtil.getPowerOf2(config.get(CLUSTER_MAX_PARTITIONS));
            hasLocalPartitions = idAuthFeatures.hasLocalKeyPartition();

            placementStrategy = Backend.getImplementationClass(config, config.get(PLACEMENT_STRATEGY),
                    REGISTERED_PLACEMENT_STRATEGIES);
        } else {
            boolean storeWantsPartitioning = idAuthFeatures.isKeyOrdered() && idAuthFeatures.isDistributed();
            if (storeWantsPartitioning)
                log.warn("ID Partitioning is disabled, which will likely cause uneven data distribution and sequentially increasing keys");
            //Use the default placement strategy
            partitionBits = 0;
            hasLocalPartitions = false;
            placementStrategy = new DefaultPlacementStrategy(0);
        }
        log.debug("Partition IDs? [{}], Local Partitions? [{}]",partitionIDs,hasLocalPartitions);
        idManager = new IDManager(partitionBits);
        Preconditions.checkArgument(idManager.getPartitionBound() <= Integer.MAX_VALUE && idManager.getPartitionBound()>0);
        this.partitionIdBound = (int)idManager.getPartitionBound();

        long baseBlockSize = config.get(IDS_BLOCK_SIZE);
        idAuthority.setIDBlockSizer(new SimpleVertexIDBlockSizer(baseBlockSize));

        renewTimeoutMS = config.get(IDS_RENEW_TIMEOUT);
        renewBufferPercentage = config.get(IDS_RENEW_BUFFER_PERCENTAGE);

        idPools = new ConcurrentHashMap<Integer, PartitionIDPool>(partitionIdBound);
        schemaIdPool = new StandardIDPool(idAuthority, IDManager.SCHEMA_PARTITION, PoolType.SCHEMA.getIDNamespace(),
                idManager.getSchemaCountBound(), renewTimeoutMS, renewBufferPercentage);
        if (partitionIDs) {
            partitionVertexIdPool = new StandardIDPool(idAuthority, IDManager.PARTITIONED_VERTEX_PARTITION, PoolType.PARTITIONED_VERTEX.getIDNamespace(),
                    PoolType.PARTITIONED_VERTEX.getCountBound(idManager), renewTimeoutMS, renewBufferPercentage);
        } else {
            partitionVertexIdPool = null;
        }


        setLocalPartitions(partitionBits);
    }

    private void setLocalPartitionsToGlobal(int partitionBits) {
        placementStrategy.setLocalPartitionBounds(PartitionIDRange.getGlobalRange(partitionBits));
    }

    private void setLocalPartitions(int partitionBits) {
        if (!hasLocalPartitions) {
            setLocalPartitionsToGlobal(partitionBits);
        } else {
            List<PartitionIDRange> partitionRanges = ImmutableList.of();
            try {
                partitionRanges = PartitionIDRange.getIDRanges(partitionBits,idAuthority.getLocalIDPartition());
            } catch (Throwable e) {
                log.error("Could not process local id partitions",e);
            }

            if (!partitionRanges.isEmpty()) {
                log.info("Setting individual partition bounds: {}", partitionRanges);
                placementStrategy.setLocalPartitionBounds(partitionRanges);
            } else {
                setLocalPartitionsToGlobal(partitionBits);
            }
        }
    }

    public IDManager getIDManager() {
        return idManager;
    }

    public synchronized void close() {
        schemaIdPool.close();
        for (PartitionIDPool pool : idPools.values()) {
            pool.close();
        }
        idPools.clear();
    }

    public void assignID(InternalRelation relation) {
        assignID(relation,null);
    }

    public void assignID(InternalVertex vertex, VertexLabel label) {
        Preconditions.checkArgument(vertex!=null && label!=null);
        assignID(vertex,getVertexIDType(label));
    }


    private void assignID(InternalElement element, IDManager.VertexIDType vertexIDType) {
        for (int attempt = 0; attempt < MAX_PARTITION_RENEW_ATTEMPTS; attempt++) {
            long partitionID = -1;
            if (element instanceof TitanSchemaVertex) {
                partitionID = IDManager.SCHEMA_PARTITION;
            } else if (element instanceof TitanVertex) {
                if (vertexIDType== IDManager.VertexIDType.PartitionedVertex)
                    partitionID = IDManager.PARTITIONED_VERTEX_PARTITION;
                else
                    partitionID = placementStrategy.getPartition(element);
            } else if (element instanceof InternalRelation) {
                InternalRelation relation = (InternalRelation)element;
                if (attempt < relation.getLen()) { //On the first attempts, try to use partition of incident vertices
                    InternalVertex incident = relation.getVertex(attempt);
                    Preconditions.checkArgument(incident.hasId());
                    if (!IDManager.VertexIDType.PartitionedVertex.is(incident.getLongId()) || relation.isProperty()) {
                        partitionID = getPartitionID(incident);
                    } else {
                        continue;
                    }
                } else {
                    partitionID = placementStrategy.getPartition(element);
                }
            }
            try {
                assignID(element, partitionID, vertexIDType);
            } catch (IDPoolExhaustedException e) {
                continue; //try again on a different partition
            }
            assert element.hasId();

            //Check if we should assign a different representative of a potential partitioned vertex
            if (element instanceof InternalRelation) {
                InternalRelation relation = (InternalRelation)element;
                long move2Partition = -1;
                if (relation.isEdge()) {
                    //If one end vertex is partitioned and the other isn't, use the partition of the non-partitioned vertex
                    for (int pos = 0; pos < relation.getArity(); pos++) {
                        int otherpos = (pos+1)%2;
                        if (idManager.isPartitionedVertex(relation.getVertex(pos).getLongId()) &&
                                !idManager.isPartitionedVertex(relation.getVertex(otherpos).getLongId())) {
                            move2Partition = getPartitionID(relation.getVertex(otherpos));
                        }
                    }
                }
                for (int pos = 0; pos < relation.getArity(); pos++) {
                    InternalVertex incident = relation.getVertex(pos);
                    long newPartition;
                    if (idManager.isPartitionedVertex(incident.getLongId())) {
                        if (((InternalRelationType)relation.getType()).getMultiplicity().isUnique(EdgeDirection.fromPosition(pos))) {
                            //If the relation is unique in the direction, we assign it to the canonical vertex...
                            newPartition = idManager.getPartitionId(idManager.getCanonicalVertexId(incident.getLongId()));
                        } else if (move2Partition>=0) {
                            //...else, we assign it to the partition of the non-partitioned vertex...
                            newPartition = move2Partition;
                        } else {
                            //...and if such does not exists (i.e. property or both end vertices are partitioned) we use the hash of the relation id
                            assert (relation.isProperty() && ((TitanProperty)relation).getPropertyKey().getCardinality()!=Cardinality.SINGLE) ||
                                    (relation.isEdge() && idManager.isPartitionedVertex(relation.getVertex(0).getLongId()) && idManager.isPartitionedVertex(relation.getVertex(1).getLongId()));
                            newPartition = idManager.getPartitionHashForId(relation.getLongId());
                        }
                        if (idManager.getPartitionId(incident.getLongId())!=newPartition) {
                            ((ReassignableRelation)relation).setVertexAt(pos,incident.tx().getOtherPartitionVertex(incident, newPartition));
                        }
                    }
                }
            }
            return;
        }
        throw new IDPoolExhaustedException("Could not find non-exhausted partition ID Pool after " + MAX_PARTITION_RENEW_ATTEMPTS + " attempts");
    }

    public void assignIDs(Iterable<InternalRelation> addedRelations) {
        if (!placementStrategy.supportsBulkPlacement()) {
            for (InternalRelation relation : addedRelations) {
                for (int i = 0; i < relation.getArity(); i++) {
                    InternalVertex vertex = relation.getVertex(i);
                    if (!vertex.hasId()) {
                        assignID(vertex, getVertexIDType(vertex));
                    }
                }
                assignID(relation);
            }
        } else {
            //2) only assign ids to (user) vertices
            Map<InternalVertex, PartitionAssignment> assignments = new HashMap<InternalVertex, PartitionAssignment>();
            for (InternalRelation relation : addedRelations) {
                for (int i = 0; i < relation.getArity(); i++) {
                    InternalVertex vertex = relation.getVertex(i);
                    if (!vertex.hasId()) {
                        assert !(vertex instanceof TitanSchemaVertex); //Those are assigned ids immediately in the transaction
                        if (vertex.getVertexLabel().isPartitioned())
                            assignID(vertex, getVertexIDType(vertex)); //Assign partitioned vertex ids immediately
                        else
                            assignments.put(vertex, PartitionAssignment.EMPTY);
                    }
                }
            }
            log.trace("Bulk id assignment for {} vertices", assignments.size());
            for (int attempt = 0; attempt < MAX_PARTITION_RENEW_ATTEMPTS && (assignments != null && !assignments.isEmpty()); attempt++) {
                placementStrategy.getPartitions(assignments);
                Map<InternalVertex, PartitionAssignment> leftOvers = null;
                Iterator<Map.Entry<InternalVertex, PartitionAssignment>> iter = assignments.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<InternalVertex, PartitionAssignment> entry = iter.next();
                    try {
                        assignID(entry.getKey(), entry.getValue().getPartitionID(), getVertexIDType(entry.getKey()));
                        Preconditions.checkArgument(entry.getKey().hasId());
                    } catch (IDPoolExhaustedException e) {
                        if (leftOvers == null) leftOvers = new HashMap<InternalVertex, PartitionAssignment>();
                        leftOvers.put(entry.getKey(), PartitionAssignment.EMPTY);
                        break;
                    }
                }
                if (leftOvers != null) {
                    while (iter.hasNext()) leftOvers.put(iter.next().getKey(), PartitionAssignment.EMPTY);
                    log.debug("Exhausted ID Pool in bulk assignment. Left-over vertices {}", leftOvers.size());
                }
                assignments = leftOvers;
            }
            if (assignments != null && !assignments.isEmpty())
                throw new IDPoolExhaustedException("Could not find non-exhausted partition ID Pool after " + MAX_PARTITION_RENEW_ATTEMPTS + " attempts");
            //3) assign ids to relations
            for (InternalRelation relation : addedRelations) {
                assignID(relation);
            }
        }
    }

    private final long getPartitionID(final InternalVertex v) {
        long vid = v.getLongId();
        if (IDManager.VertexIDType.Schema.is(vid)) return IDManager.SCHEMA_PARTITION;
        else return idManager.getPartitionId(vid);
    }

    private void assignID(final InternalElement element, final long partitionIDl, final IDManager.VertexIDType userVertexIDType) {
        Preconditions.checkNotNull(element);
        Preconditions.checkArgument(!element.hasId());
        Preconditions.checkArgument((element instanceof TitanRelation) ^ (userVertexIDType!=null));
        Preconditions.checkArgument(partitionIDl >= 0 && partitionIDl < partitionIdBound, partitionIDl);
        final int partitionID = (int) partitionIDl;

        long count;
        if (element instanceof TitanSchemaVertex) {
            Preconditions.checkArgument(partitionID==IDManager.SCHEMA_PARTITION);
            count = schemaIdPool.nextID();
        } else if (userVertexIDType==IDManager.VertexIDType.PartitionedVertex) {
            Preconditions.checkArgument(partitionID==IDManager.PARTITIONED_VERTEX_PARTITION);
            Preconditions.checkArgument(partitionVertexIdPool!=null);
            count = partitionVertexIdPool.nextID();
        } else {
            PartitionIDPool partitionPool = idPools.get(partitionID);
            if (partitionPool == null) {
                partitionPool = new PartitionIDPool(partitionID, idAuthority, idManager, renewTimeoutMS, renewBufferPercentage);
                idPools.putIfAbsent(partitionID,partitionPool);
                partitionPool = idPools.get(partitionID);
            }
            Preconditions.checkNotNull(partitionPool);
            if (partitionPool.isExhausted()) {
                placementStrategy.exhaustedPartition(partitionID);
                throw new IDPoolExhaustedException("Exhausted id pool for partition: " + partitionID);
            }
            IDPool idPool;
            if (element instanceof TitanRelation) {
                idPool = partitionPool.getPool(PoolType.RELATION);
            } else {
                Preconditions.checkArgument(userVertexIDType!=null);
                idPool = partitionPool.getPool(PoolType.getPoolTypeFor(userVertexIDType));
            }
            try {
                count = idPool.nextID();
                partitionPool.accessed();
            } catch (IDPoolExhaustedException e) {
                log.debug("Pool exhausted for partition id {}", partitionID);
                placementStrategy.exhaustedPartition(partitionID);
                partitionPool.exhaustedIdPool();
                throw e;
            }
        }

        long elementId;
        if (element instanceof InternalRelation) {
            elementId = idManager.getRelationID(count, partitionID);
        } else if (element instanceof PropertyKey) {
            elementId = idManager.getSchemaId(IDManager.VertexIDType.UserPropertyKey,count);
        } else if (element instanceof EdgeLabel) {
            elementId = idManager.getSchemaId(IDManager.VertexIDType.UserEdgeLabel, count);
        } else if (element instanceof VertexLabel) {
            elementId = idManager.getSchemaId(IDManager.VertexIDType.VertexLabel, count);
        } else if (element instanceof TitanSchemaVertex) {
            elementId = idManager.getSchemaId(IDManager.VertexIDType.GenericSchemaType,count);
        } else {
            elementId = idManager.getVertexID(count, partitionID, userVertexIDType);
        }

        Preconditions.checkArgument(elementId >= 0);
        element.setId(elementId);
    }

    private static IDManager.VertexIDType getVertexIDType(VertexLabel vlabel) {
        if (vlabel.isPartitioned()) {
            return IDManager.VertexIDType.PartitionedVertex;
        } else if (vlabel.isStatic()) {
            return IDManager.VertexIDType.UnmodifiableVertex;
        } else {
            return IDManager.VertexIDType.NormalVertex;
        }
    }

    private static IDManager.VertexIDType getVertexIDType(TitanVertex v) {
        return getVertexIDType(v.getVertexLabel());
    }

    private class SimpleVertexIDBlockSizer implements IDBlockSizer {

        private final long baseBlockSize;

        SimpleVertexIDBlockSizer(final long size) {
            Preconditions.checkArgument(size > 0 && size < Integer.MAX_VALUE);
            this.baseBlockSize = size;
        }

        @Override
        public long getBlockSize(int idNamespace) {
            switch (PoolType.getPoolType(idNamespace)) {
                case NORMAL_VERTEX:
                    return baseBlockSize;
                case UNMODIFIABLE_VERTEX:
                    return Math.max(10,baseBlockSize/10);
                case PARTITIONED_VERTEX:
                    return Math.max(10,baseBlockSize/100);
                case RELATION:
                    return baseBlockSize * 8;
                case SCHEMA:
                    return 50;

                default:
                    throw new IllegalArgumentException("Unrecognized pool type");
            }
        }

        @Override
        public long getIdUpperBound(int idNamespace) {
            return PoolType.getPoolType(idNamespace).getCountBound(idManager);
        }
    }

    private enum PoolType {

        NORMAL_VERTEX, UNMODIFIABLE_VERTEX, PARTITIONED_VERTEX, RELATION, SCHEMA;

        public int getIDNamespace() {
            return ordinal();
        }

        public long getCountBound(IDManager idManager) {
            switch (this) {
                case NORMAL_VERTEX:
                case UNMODIFIABLE_VERTEX:
                case PARTITIONED_VERTEX:
                    return idManager.getVertexCountBound();
                case RELATION: return idManager.getRelationCountBound();
                case SCHEMA: return idManager.getSchemaCountBound();
                default: throw new AssertionError("Unrecognized type: " + this);
            }
        }

        public boolean hasOnePerPartition() {
            switch(this) {
                case NORMAL_VERTEX:
                case UNMODIFIABLE_VERTEX:
                case RELATION:
                    return true;
                default: return false;
            }
        }

        public static PoolType getPoolTypeFor(IDManager.VertexIDType idType) {
            if (idType==IDManager.VertexIDType.NormalVertex) return NORMAL_VERTEX;
            else if (idType== IDManager.VertexIDType.UnmodifiableVertex) return UNMODIFIABLE_VERTEX;
            else if (idType== IDManager.VertexIDType.PartitionedVertex) return PARTITIONED_VERTEX;
            else if (IDManager.VertexIDType.Schema.isSubType(idType)) return SCHEMA;
            else throw new IllegalArgumentException("Invalid id type: " + idType);
        }

        public static PoolType getPoolType(int idNamespace) {
            Preconditions.checkArgument(idNamespace>=0 && idNamespace<values().length);
            return values()[idNamespace];
        }

    }

    private static class PartitionIDPool extends EnumMap<PoolType,IDPool> {

        private volatile long lastAccess;
        private volatile boolean exhausted;

        PartitionIDPool(int partitionID, IDAuthority idAuthority, IDManager idManager, Duration renewTimeoutMS, double renewBufferPercentage) {
            super(PoolType.class);
            for (PoolType type : PoolType.values()) {
                if (!type.hasOnePerPartition()) continue;
                put(type,new StandardIDPool(idAuthority, partitionID, type.getIDNamespace(), type.getCountBound(idManager), renewTimeoutMS, renewBufferPercentage));
            }
        }

        public IDPool getPool(PoolType type) {
            Preconditions.checkArgument(!exhausted && type.hasOnePerPartition());
            return super.get(type);
        }

        public void close() {
            for (IDPool pool : values()) pool.close();
            super.clear();
        }

        public void exhaustedIdPool() {
            exhausted = true;
            close();
        }

        public boolean isExhausted() {
            return exhausted;
        }

        public void accessed() {
            lastAccess = System.currentTimeMillis();
        }

        public long getLastAccess() {
            return lastAccess;
        }

    }



}
