package com.thinkaurelius.titan.graphdb.database.idassigner;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.IDAuthority;
import com.thinkaurelius.titan.diskstorage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class StandardIDPool implements IDPool {

    private static final Logger log =
            LoggerFactory.getLogger(StandardIDPool.class);

    private static final long BUFFER_EMPTY = -1;
    private static final long BUFFER_POOL_EXHAUSTION = -100;
    
    private static final int RENEW_ID_COUNT = 100;
    
    private static final int MAX_WAIT_TIME = 2000;
    
    private final IDAuthority idAuthority;
    private final long maxID; //inclusive
    private final int partitionID;
        
    private long nextID;
    private long currentMaxID;
    private long renewBufferID;
    
    private volatile long bufferNextID;
    private volatile long bufferMaxID;
    private Thread idBlockRenewer;

    private boolean initialized;
    
    public StandardIDPool(IDAuthority idAuthority, long partitionID, long maximumID) {
        Preconditions.checkArgument(maximumID>0);
        this.idAuthority = idAuthority;
        this.partitionID=(int)partitionID;
        this.maxID = maximumID;

        nextID = 0;
        currentMaxID = 0;
        renewBufferID = 0;
        
        bufferNextID = BUFFER_EMPTY;
        bufferMaxID = BUFFER_EMPTY;
        idBlockRenewer = null;

        initialized=false;
    }
    
    private synchronized void nextBlock() throws InterruptedException {
        assert nextID == currentMaxID;
        
        long time = System.currentTimeMillis();
        if (idBlockRenewer!=null && idBlockRenewer.isAlive()) {
            //Updating thread has not yet completed, so wait for it
            log.debug("Waiting for id renewal thread");
            idBlockRenewer.join(MAX_WAIT_TIME);
            if (idBlockRenewer.isAlive()) throw new IllegalStateException("ID renewal thread did not complete in time.");
        }
        if (bufferMaxID==BUFFER_POOL_EXHAUSTION || bufferNextID==BUFFER_POOL_EXHAUSTION)
            throw new IDPoolExhaustedException("Exhausted ID Pool for partition: " + partitionID);

        Preconditions.checkArgument(bufferMaxID>0,bufferMaxID);
        Preconditions.checkArgument(bufferNextID>0,bufferNextID);
        
        nextID = bufferNextID;
        currentMaxID = bufferMaxID;
        
        log.debug("[{}] Next/Max ID: {}",partitionID,new long[]{nextID, currentMaxID});

        assert nextID>0 && currentMaxID >nextID;
        
        bufferNextID = BUFFER_EMPTY;
        bufferMaxID = BUFFER_EMPTY;
        
        renewBufferID = currentMaxID - Math.max(RENEW_ID_COUNT, (currentMaxID - nextID)/5);
        if (renewBufferID>= currentMaxID) renewBufferID = currentMaxID -1;
        if (renewBufferID<nextID) renewBufferID = nextID;
        assert renewBufferID>=nextID && renewBufferID< currentMaxID;
    }
    
    private void renewBuffer() {
        Preconditions.checkArgument(bufferNextID==BUFFER_EMPTY,bufferNextID);
        Preconditions.checkArgument(bufferMaxID==BUFFER_EMPTY,bufferMaxID);
        try {
            long[] idblock = idAuthority.getIDBlock(partitionID);
            bufferNextID = idblock[0];
            bufferMaxID = idblock[1];
            Preconditions.checkArgument(bufferNextID>0,bufferNextID);
            Preconditions.checkArgument(bufferMaxID>bufferNextID,bufferMaxID);
        } catch (StorageException e) {
            throw new TitanException("Could not acquire new ID block from storage",e);
        } catch (IDPoolExhaustedException e) {
            bufferNextID=BUFFER_POOL_EXHAUSTION;
            bufferMaxID=BUFFER_POOL_EXHAUSTION;
        }
    }

    @Override
    public synchronized long nextID() {
        assert nextID<= currentMaxID;
        if (!initialized) {
            renewBuffer();
            initialized=true;
        }

        if (nextID== currentMaxID) {
            try {
                nextBlock();
            } catch (InterruptedException e) {
                throw new TitanException("Could not renew id block due to interruption",e);
            }
        }

        if (nextID == renewBufferID) {
            Preconditions.checkArgument(idBlockRenewer==null || !idBlockRenewer.isAlive(),idBlockRenewer);
            //Renew buffer
            log.debug("Starting id block renewal thread upon {}",nextID);
            idBlockRenewer = new IDBlockThread();
            idBlockRenewer.start();
        }
        long returnId = nextID;
        nextID++;
        if (returnId>maxID) throw new IDPoolExhaustedException("Exhausted max id of " + maxID);
        log.trace("[{}] Returned id: {}",partitionID,returnId);
        return returnId;
    }

    @Override
    public synchronized void close() {
        if (idBlockRenewer!=null && idBlockRenewer.isAlive()) {
            log.debug("ID renewal thread still alive on close");

            //Wait for renewer to finish
            try {
                idBlockRenewer.join(5000);
            } catch (InterruptedException e) {
                throw new TitanException("Interrupted while waiting for id renewer thread to finish",e);
            }
            if (idBlockRenewer.isAlive()) {
                throw new TitanException("ID renewer thread did not finish");
            }
        }
    }

    private class IDBlockThread extends Thread {

        @Override
        public void run() {
            renewBuffer();
            log.debug("Finishing id renewal thread");
        }

    }

}
