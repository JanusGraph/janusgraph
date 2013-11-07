package com.thinkaurelius.titan.graphdb.database.idassigner;


import cern.colt.list.ObjectArrayList;
import cern.colt.map.AbstractIntObjectMap;
import cern.colt.map.OpenIntObjectHashMap;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.diskstorage.IDAuthority;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.DefaultPlacementStrategy;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.IDPlacementStrategy;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.PartitionAssignment;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;


public class VertexIDAssigner {

    private static final Logger log =
            LoggerFactory.getLogger(VertexIDAssigner.class);

    private static final Object EXHAUSTED_ID_POOL = new Object();

    private static final int DEFAULT_PARTITION_BITS = 30;
    private static final int MAX_PARTITION_RENEW_ATTEMPTS = 1000;
    private static final int DEFAULT_PARTITION = 0;

    final AbstractIntObjectMap idPools;
    final ReadWriteLock idPoolsLock;

    private final IDAuthority idAuthority;
    private final IDManager idManager;
    private final IDPlacementStrategy placementStrategy;

    //For StandardIDPool
    private final long renewTimeoutMS;
    private final double renewBufferPercentage;

    private final int maxPartitionID;
    private final boolean hasLocalPartitions;


    public VertexIDAssigner(Configuration config, IDAuthority idAuthority, StoreFeatures idAuthFeatures) {
        Preconditions.checkNotNull(idAuthority);
        this.idAuthority = idAuthority;

        long partitionBits;
        boolean partitionIDs = config.getBoolean(IDS_PARTITION_KEY, IDS_PARTITION_DEFAULT);
        if (partitionIDs) {
            //Use a placement strategy that balances partitions
            partitionBits = DEFAULT_PARTITION_BITS;
            hasLocalPartitions = idAuthFeatures.hasLocalKeyPartition();
            placementStrategy = new SimpleBulkPlacementStrategy(config);
        } else {
            if (idAuthFeatures.isKeyOrdered() && idAuthFeatures.isDistributed())
                log.warn("ID Partitioning is disabled which will likely cause uneven data distribution");
            //Use the default placement strategy
            partitionBits = 0;
            hasLocalPartitions = false;
            placementStrategy = new DefaultPlacementStrategy(0);
        }
        log.debug("Partition IDs? [{}], Local Partitions? [{}]",partitionIDs,hasLocalPartitions);
        idManager = new IDManager(partitionBits);
        Preconditions.checkArgument(idManager.getMaxPartitionCount() < Integer.MAX_VALUE);
        this.maxPartitionID = (int) idManager.getMaxPartitionCount();

        long baseBlockSize = config.getLong(IDS_BLOCK_SIZE_KEY, IDS_BLOCK_SIZE_DEFAULT);
        idAuthority.setIDBlockSizer(new SimpleVertexIDBlockSizer(baseBlockSize));

        renewTimeoutMS = config.getLong(IDS_RENEW_TIMEOUT_KEY,IDS_RENEW_TIMEOUT_DEFAULT);
        renewBufferPercentage = config.getDouble(IDS_RENEW_BUFFER_PERCENTAGE_KEY,IDS_RENEW_BUFFER_PERCENTAGE_DEFAULT);

        idPools = new OpenIntObjectHashMap();
        idPoolsLock = new ReentrantReadWriteLock();

        setLocalPartitions();
    }

    private void setLocalPartitions() {
        if (!hasLocalPartitions) {
            placementStrategy.setLocalPartitionBounds(0, maxPartitionID + 1, maxPartitionID + 1);
        } else {
            StaticBuffer[] local = null;
            try {
                local = idAuthority.getLocalIDPartition();
            } catch (Exception e) {
                log.error("Could not read local id partition: {}", e);
                placementStrategy.setLocalPartitionBounds(0, maxPartitionID + 1, maxPartitionID + 1);
            }
            if (local != null) {
                Preconditions.checkArgument(local[0].length() >= 4 && local[1].length() >= 4);
                int[] partition = new int[2];
                for (int i = 0; i < 2; i++) {
                    partition[i] = local[i].getInt(0);
                }
                //Adjust lower end if necessary (needs to be inclusive)
                if ((partition[0] & 3) > 0) partition[0] = (partition[0] >>> 2) + 1;
                else partition[0] = (partition[0] >>> 2);
                //Upper bound needs to be exclusive
                partition[1] = (partition[1] >>> 2) - 1;
                Preconditions.checkArgument(partition[0] != partition[1]);
                log.info("Setting partition bound [{},{}]", partition[0], partition[1]);
                placementStrategy.setLocalPartitionBounds(partition[0], partition[1], maxPartitionID + 1);
            }
        }
    }

    public IDManager getIDManager() {
        return idManager;
    }

    public synchronized void close() {
        idPoolsLock.writeLock().lock();
        try {
            ObjectArrayList pools = idPools.values();
            for (int i = 0; i < pools.size(); i++) {
                ((PartitionPool) pools.get(i)).close();
            }
            idPools.clear();
        } finally {
            idPoolsLock.writeLock().unlock();
        }
    }

    public void assignID(InternalElement vertex) {
        for (int attempt = 0; attempt < MAX_PARTITION_RENEW_ATTEMPTS; attempt++) {
            long partitionID = -1;
            if (vertex instanceof InternalRelation) {
                partitionID = placementStrategy.getPartition(vertex);
            } else if (vertex instanceof TitanType) {
                partitionID = DEFAULT_PARTITION;
            } else {
                partitionID = placementStrategy.getPartition(vertex);
            }
            try {
                assignID(vertex, partitionID);
                return;
            } catch (IDPoolExhaustedException e) {
            }
        }
        throw new IDPoolExhaustedException("Could not find non-exhausted partition ID Pool after " + MAX_PARTITION_RENEW_ATTEMPTS + " attempts");
    }

    public void assignIDs(Iterable<InternalRelation> addedRelations) {
        if (!placementStrategy.supportsBulkPlacement()) {
            for (InternalRelation relation : addedRelations) {
                for (int i = 0; i < relation.getArity(); i++) {
                    InternalVertex vertex = relation.getVertex(i);
                    if (!vertex.hasId()) {
                        assignID(vertex);
                    }
                }
                assignID(relation);
            }
        } else {
            //First, only assign idAuthorities to (real) vertices and types
            Map<InternalVertex, PartitionAssignment> assignments = new HashMap<InternalVertex, PartitionAssignment>();
            for (InternalRelation relation : addedRelations) {
                for (int i = 0; i < relation.getArity(); i++) {
                    InternalVertex vertex = relation.getVertex(i);
                    if (!vertex.hasId()) {
                        if (vertex instanceof TitanType) {
                            assignID(vertex, DEFAULT_PARTITION);
                        } else {
                            assignments.put(vertex, PartitionAssignment.EMPTY);
                        }
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
                        assignID(entry.getKey(), entry.getValue().getPartitionID());
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
            //Second, assign idAuthorities to relations
            for (InternalRelation relation : addedRelations) {
                for (int pos = 0; pos < relation.getArity(); pos++) {
                    try {
                        Preconditions.checkArgument(relation.getVertex(pos).hasId());
                        assignID(relation, getPartitionID(relation.getVertex(pos)));
                        break;
                    } catch (IDPoolExhaustedException e) {
                    }
                }
                if (!relation.hasId()) assignID(relation);
            }
        }
    }

    private final long getPartitionID(final InternalVertex v) {
        long vid = v.getID();
        if (IDManager.IDType.TitanType.is(vid)) return 0;
        else return idManager.getPartitionID(vid);
    }

    private void assignID(final InternalElement vertex, final long partitionIDl) {
        Preconditions.checkNotNull(vertex);
        Preconditions.checkArgument(!vertex.hasId());
        Preconditions.checkArgument(partitionIDl >= 0 && partitionIDl <= maxPartitionID, partitionIDl);
        final int partitionID = (int) partitionIDl;
        long id = -1;

        Object poolObj = null;
        idPoolsLock.readLock().lock();
        try {
            poolObj = idPools.get(partitionID);
        } finally {
            idPoolsLock.readLock().unlock();
        }
        if (poolObj == null) {
            idPoolsLock.writeLock().lock();
            try {
                if (idPools.containsKey(partitionID)) {
                    poolObj = idPools.get(partitionID);
                } else {
                    poolObj = new PartitionPool(partitionID, idAuthority, idManager, partitionID == DEFAULT_PARTITION, renewTimeoutMS, renewBufferPercentage);
                    idPools.put(partitionID, poolObj);
                }
            } finally {
                idPoolsLock.writeLock().unlock();
            }
        }
        Preconditions.checkNotNull(poolObj);
        if (poolObj == EXHAUSTED_ID_POOL) {
            placementStrategy.exhaustedPartition(partitionID);
            throw new IDPoolExhaustedException("Exhausted id pool for partition: " + partitionID);
        } else {
            PartitionPool pool = (PartitionPool) poolObj;
            try {
                if (vertex instanceof InternalRelation) {
                    id = idManager.getRelationID(pool.relation.nextID(), partitionID);
                } else if (vertex instanceof TitanKey) {
                    id = idManager.getPropertyKeyID(pool.relationType.nextID()+SystemTypeManager.SYSTEM_TYPE_OFFSET);
                } else if (vertex instanceof TitanLabel) {
                    id = idManager.getEdgeLabelID(pool.relationType.nextID()+SystemTypeManager.SYSTEM_TYPE_OFFSET);
                } else {
                    id = idManager.getVertexID(pool.vertex.nextID(), partitionID);
                }
                pool.accessed();
            } catch (IDPoolExhaustedException e) {
                log.debug("Pool exhausted for partition id {}", partitionID);
                placementStrategy.exhaustedPartition(partitionID);
                //Close and remove pool
                idPoolsLock.writeLock().lock();
                try {
                    idPools.put(partitionID, EXHAUSTED_ID_POOL);
                    pool.close();
                } finally {
                    idPoolsLock.writeLock().unlock();
                }
                throw e;
            }
        }
        Preconditions.checkArgument(id >= 0);
        vertex.setID(id);
    }

    private class SimpleVertexIDBlockSizer implements IDBlockSizer {

        private static final int AVG_EDGES_PER_VERTEX = 10;
        private static final int DEFAULT_NUM_EDGE_TYPES = 12;

        private final long baseBlockSize;

        SimpleVertexIDBlockSizer(final long size) {
            Preconditions.checkArgument(size > 0 && size < Integer.MAX_VALUE);
            this.baseBlockSize = size;
        }

        @Override
        public long getBlockSize(int fullPartitionID) {
            switch (PoolType.getPoolType(fullPartitionID)) {
                case VERTEX:
                    return baseBlockSize;
                case RELATION:
                    return baseBlockSize * AVG_EDGES_PER_VERTEX;
                case RELATIONTYPE:
                    return DEFAULT_NUM_EDGE_TYPES;
                default:
                    throw new IllegalArgumentException("Unrecognized pool type");
            }
        }

        @Override
        public long getIdUpperBound(int fullPartitionID) {
            switch (PoolType.getPoolType(fullPartitionID)) {
                case VERTEX:
                    return idManager.getMaxVertexCount()+1;
                case RELATION:
                    return idManager.getMaxRelationCount()+1;
                case RELATIONTYPE:
                    return idManager.getMaxTitanTypeCount()+1;
                default:
                    throw new IllegalArgumentException("Unrecognized pool type");
            }
        }
    }

    private static class PartitionPool {

        final IDPool vertex;
        final IDPool relation;
        final IDPool relationType;

        long lastAccess;

        PartitionPool(int partitionID, IDAuthority idAuthority, IDManager idManager, boolean includeRelationType, long renewTimeoutMS, double renewBufferPercentage) {
            vertex = new StandardIDPool(idAuthority, PoolType.VERTEX.getFullPartitionID(partitionID), idManager.getMaxVertexCount(), renewTimeoutMS, renewBufferPercentage);
            relation = new StandardIDPool(idAuthority, PoolType.RELATION.getFullPartitionID(partitionID), idManager.getMaxRelationCount(), renewTimeoutMS, renewBufferPercentage);
            if (includeRelationType)
                relationType = new StandardIDPool(idAuthority, PoolType.RELATIONTYPE.getFullPartitionID(partitionID), idManager.getMaxTitanTypeCount(), renewTimeoutMS, renewBufferPercentage);
            else relationType = null;
        }

        public void close() {
            vertex.close();
            relation.close();
            if (relationType != null) relationType.close();
        }

        public void accessed() {
            lastAccess = System.currentTimeMillis();
        }

    }

    private enum PoolType {
        VERTEX, RELATION, RELATIONTYPE;

        private int getID() {
            switch (this) {
                case VERTEX:
                    return 1;
                case RELATION:
                    return 2;
                case RELATIONTYPE:
                    return 3;
                default:
                    throw new IllegalArgumentException("Unrecognized PoolType: " + this);
            }
        }

        public int getFullPartitionID(int partitionID) {
            Preconditions.checkArgument(partitionID < (1 << 30), partitionID);
            return (partitionID << 2) | getID();
        }

        public static int getPartitionID(int fullPartitionID) {
            return fullPartitionID >>> 2;
        }

        public static PoolType getPoolType(int fullPartitionID) {
            switch (fullPartitionID & 3) { // & 11b (last two bits)
                case 1:
                    return VERTEX;
                case 2:
                    return RELATION;
                case 3:
                    return RELATIONTYPE;
                default:
                    throw new IllegalArgumentException("Unrecognized partition id: " + fullPartitionID);
            }
        }

    }

}
