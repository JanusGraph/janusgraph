package com.thinkaurelius.titan.graphdb.database.idassigner;


import cern.colt.list.ObjectArrayList;
import cern.colt.map.AbstractIntObjectMap;
import cern.colt.map.OpenIntObjectHashMap;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.IDAuthority;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.DefaultPlacementStrategy;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.IDPlacementStrategy;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.PartitionAssignment;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class VertexIDAssigner {

    private static final Logger log =
            LoggerFactory.getLogger(VertexIDAssigner.class);
    
    private static final int DEFAULT_PARTITION_BITS = 30;
    private static final int MAX_PARTITION_RENEW_ATTEMPTS = 1000;
    private static final int DEFAULT_PARTITION = 0;
    
    final AbstractIntObjectMap idPools;
    final ReadWriteLock idPoolsLock;

	private final IDAuthority idAuthority;
    private final IDManager idManager;
    private final IDPlacementStrategy placementStrategy;

	private final int maxPartitionID;
    private final boolean partitionRelationTypes;
    private final boolean maintainLocalPartitions;

	
	public VertexIDAssigner(Configuration config, Backend backend) {
        Preconditions.checkNotNull(backend);
        idAuthority = backend.getIDAuthority();
        StoreFeatures storeFeatures = backend.getStoreFeatures();
        maintainLocalPartitions = storeFeatures.isDistributed() && storeFeatures.isKeyOrdered() && storeFeatures.hasLocalKeyPartition();

        final long groupBits = IDManager.defaultGroupBits;
        long partitionBits;
        if (storeFeatures.isDistributed() && storeFeatures.isKeyOrdered()) {
            //Use a placement strategy that balances partitions
            partitionBits=DEFAULT_PARTITION_BITS;
            placementStrategy = new SimpleBulkPlacementStrategy(config);
        } else {
            //Use the default placement strategy
            partitionBits=0;
            placementStrategy = new DefaultPlacementStrategy(0);
        }
        idManager = new IDManager(partitionBits,groupBits);
        Preconditions.checkArgument(idManager.getMaxPartitionID()<=Integer.MAX_VALUE);
        this.maxPartitionID = (int)idManager.getMaxPartitionID();

        partitionRelationTypes = false;
        
        long baseBlockSize = config.getLong(GraphDatabaseConfiguration.IDS_BLOCK_SIZE_KEY,GraphDatabaseConfiguration.IDS_BLOCK_SIZE_DEFAULT);
        idAuthority.setIDBlockSizer(new SimpleVertexIDBlockSizer(baseBlockSize));

        idPools = new OpenIntObjectHashMap();
        idPoolsLock = new ReentrantReadWriteLock();

        setLocalPartitions();
	}

    private void setLocalPartitions() {
        if (!maintainLocalPartitions) {
            placementStrategy.setLocalPartitionBounds(0,maxPartitionID+1);
        } else {
            try {
                log.debug("Attempting to retrieve local partition");
                ByteBuffer[] local = idAuthority.getLocalIDPartition();
                Preconditions.checkArgument(local[0].remaining()>=4 && local[1].remaining()>=4);
                int[] partition = new int[2];
                for (int i=0;i<2;i++) {
                    local[i].order(ByteOrder.BIG_ENDIAN);
                    local[i].mark();
                    partition[i]=local[i].getInt();
                    local[i].reset();
                }
                //Adjust lower end if necessary (needs to be inclusive)
                if ((partition[0] & 3) > 0) partition[0]=(partition[0]>>>2)+1;
                else partition[0]=(partition[0]>>>2);
                //Upper bound needs to be exclusive
                partition[1]=(partition[1]>>>2)-1;
                placementStrategy.setLocalPartitionBounds(partition[0],partition[1]);
            } catch (Exception e) {
                log.error("Could not read local id partition: {}",e);
                placementStrategy.setLocalPartitionBounds(0,maxPartitionID+1);
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
            for (int i=0;i<pools.size();i++) {
                ((IDPool)pools.get(i)).close();
            }
            idPools.clear();
        } finally {
            idPoolsLock.writeLock().unlock();
        }
	}

    public void assignID(InternalTitanVertex vertex) {
        for (int attempt=0;attempt<MAX_PARTITION_RENEW_ATTEMPTS;attempt++) {
            long partitionID = -1;
            if (vertex instanceof InternalRelation) {
                InternalTitanVertex start = ((InternalRelation)vertex).getVertex(0);
                if (start.hasID()) partitionID = idManager.getPartitionID(start.getID());
                else partitionID = placementStrategy.getPartition(start);
            } else if (vertex instanceof TitanType) {
                if (partitionRelationTypes) partitionID = placementStrategy.getPartition(vertex);
                else partitionID = DEFAULT_PARTITION;
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
                for (int i=0;i<relation.getArity();i++) {
                    InternalTitanVertex vertex = relation.getVertex(i);
                    if (!vertex.hasID()) {
                        assignID(vertex);
                    }
                }
                assignID(relation);
            }
        } else {
            //First, only assign idAuthorities to (real) vertices and types
            Map<InternalTitanVertex,PartitionAssignment> assignments = new HashMap<InternalTitanVertex, PartitionAssignment>();
            for (InternalRelation relation : addedRelations) {
                for (int i=0;i<relation.getArity();i++) {
                    InternalTitanVertex vertex = relation.getVertex(i);
                    if (!vertex.hasID()) {
                        if (!(vertex instanceof TitanType) || partitionRelationTypes) {
                            assignments.put(vertex,PartitionAssignment.EMPTY);
                        } else {
                            assignID(vertex,DEFAULT_PARTITION);
                        }
                    }
                }
            }
            log.debug("Bulk id assignment for {} vertices",assignments.size());
            for (int attempt=0;attempt<MAX_PARTITION_RENEW_ATTEMPTS;attempt++) {
                placementStrategy.getPartitions(assignments);
                try {
                    Iterator<Map.Entry<InternalTitanVertex,PartitionAssignment>> iter = assignments.entrySet().iterator();
                    while (iter.hasNext()) {
                        Map.Entry<InternalTitanVertex,PartitionAssignment> entry = iter.next();
                        assignID(entry.getKey(),entry.getValue().getPartitionID());
                        iter.remove();
                    }
                    break;
                } catch (IDPoolExhaustedException e) {
                }
            }
            if (!assignments.isEmpty())
                throw new IDPoolExhaustedException("Could not find non-exhausted partition ID Pool after " + MAX_PARTITION_RENEW_ATTEMPTS + " attempts");
            //Second, assign idAuthorities to relations
            for (InternalRelation relation : addedRelations) {
                Preconditions.checkArgument(relation.getVertex(0).hasID());
                assignID(relation,idManager.getPartitionID(relation.getVertex(0).getID()));
            }
        }
    }

    private void assignID(InternalTitanVertex vertex, long partitionIDl) {
        Preconditions.checkNotNull(vertex);
        Preconditions.checkArgument(!vertex.hasID());
        Preconditions.checkArgument(partitionIDl>=0 && partitionIDl<=maxPartitionID,partitionIDl);
        final int partitionID = (int)partitionIDl;
        long id = -1;

        PartitionPool pool = null;
        idPoolsLock.readLock().lock();
        try {
            pool = (PartitionPool)idPools.get(partitionID);
        } finally {
            idPoolsLock.readLock().unlock();
        }
        if (pool==null) {
            idPoolsLock.writeLock().lock();
            try {
                if (idPools.containsKey(partitionID)) {
                    pool = (PartitionPool) idPools.get(partitionID);
                } else {
                    pool = new PartitionPool(partitionID,idAuthority, idManager,partitionRelationTypes || partitionID==DEFAULT_PARTITION);
                    idPools.put(partitionID,idAuthority);
                }
            } finally {
                idPoolsLock.writeLock().unlock();
            }
        }
        Preconditions.checkNotNull(pool);
        try {
            if (vertex instanceof InternalRelation) {
                id = idManager.getEdgeID(pool.relation.nextID(),partitionID);
            } else if (vertex instanceof TitanKey) {
                id = idManager.getPropertyTypeID(pool.relationType.nextID(), ((TitanType)vertex).getGroup().getID(), 0);
            } else if (vertex instanceof TitanLabel) {
                id = idManager.getRelationshipTypeID(pool.relationType.nextID(), ((TitanType) vertex).getGroup().getID(), 0);
            } else {
                id = idManager.getNodeID(pool.vertex.nextID(),partitionID);
            }
            pool.accessed();
        } catch (IDPoolExhaustedException e) {
            log.debug("Pool exhausted for partition id {}",partitionID);
            placementStrategy.exhaustedPartition(partitionID);
            //Close and remove pool
            idPoolsLock.writeLock().lock();
            try {
                idPools.removeKey(partitionID);
                pool.close();
            } finally {
                idPoolsLock.writeLock().unlock();
            }
            throw e;
        }
        Preconditions.checkArgument(id>=0);
        vertex.setID(id);
    }
	
	private static class SimpleVertexIDBlockSizer implements IDBlockSizer {

        private static final int AVG_EDGES_PER_VERTEX = 10;
        private static final int DEFAULT_NUM_EDGE_TYPES = 12;

        private final long baseBlockSize;

        SimpleVertexIDBlockSizer(final long size) {
            Preconditions.checkArgument(size>0 && size<Integer.MAX_VALUE);
            this.baseBlockSize=size;
        }
        
        @Override
        public long getBlockSize(int fullPartitionID) {
            switch(PoolType.getPoolType(fullPartitionID)) {
                case VERTEX: return baseBlockSize;
                case RELATION: return baseBlockSize*AVG_EDGES_PER_VERTEX;
                case RELATIONTYPE: return DEFAULT_NUM_EDGE_TYPES;
                default: throw new IllegalArgumentException("Unrecognized pool type");
            }
        }
    }

    private static class PartitionPool {

        final IDPool vertex;
        final IDPool relation;
        final IDPool relationType;

        long lastAccess;

        PartitionPool(int partitionID, IDAuthority idAuthority, IDManager idManager, boolean includeRelationType) {
            vertex = new StandardIDPool(idAuthority,PoolType.VERTEX.getFullPartitionID(partitionID),idManager.getMaxNodeID());
            relation = new StandardIDPool(idAuthority,PoolType.RELATION.getFullPartitionID(partitionID),idManager.getMaxEdgeID());
            if (includeRelationType)
                relationType = new StandardIDPool(idAuthority,PoolType.RELATIONTYPE.getFullPartitionID(partitionID),idManager.getMaxEdgeTypeID());
            else relationType = null;
        }

        public void close() {
            vertex.close();
            relation.close();
            if (relationType!=null) relationType.close();
        }
        
        public void accessed() {
            lastAccess = System.currentTimeMillis();
        }

    }
    
    private enum PoolType {
        VERTEX, RELATION, RELATIONTYPE;
        
        private int getID() {
            switch(this) {
                case VERTEX:return 1;
                case RELATION: return 2;
                case RELATIONTYPE: return 3;
                default: throw new IllegalArgumentException("Unrecognized PoolType: " + this);
            }
        }
        
        public int getFullPartitionID(int partitionID) {
            Preconditions.checkArgument(partitionID<(2<<30));
            return (partitionID<<2) | getID();
        }
        
        public static int getPartitionID(int fullPartitionID) {
            return fullPartitionID>>>2;
        }

        public static PoolType getPoolType(int fullPartitionID) {
            switch (fullPartitionID & 3) { // & 11b (last two bits)
                case 1: return VERTEX;
                case 2: return RELATION;
                case 3: return RELATIONTYPE;
                default: throw new IllegalArgumentException("Unrecognized partition id: " + fullPartitionID);
            }
        }
        
    }

}
