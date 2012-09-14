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
    
    private static final int RENEW_ID_COUNT = 100;
    
    private static final int MAX_WAIT_TIME = 2000;
    
    private final IDAuthority idAuthority;
    //private final int blockSize;
    private final int partitionID;
        
    private long nextID;
    private long maxID;
    private long renewBufferID;
    
    private volatile long bufferNextID;
    private volatile long bufferMaxID;
    private Thread idBlockRenewer;

    private boolean initialized;
    
    public StandardIDPool(IDAuthority idAuthority, long partitionID) {
        Preconditions.checkArgument(partitionID>=0);
        Preconditions.checkArgument(partitionID<Integer.MAX_VALUE);
        this.idAuthority = idAuthority;
        this.partitionID=(int)partitionID;
        
        nextID = 0;
        maxID = 0;
        renewBufferID = 0;
        
        bufferNextID = -1;
        bufferMaxID = -1;
        idBlockRenewer = null;

        initialized=false;
    }
    
    private synchronized void nextBlock() throws InterruptedException {
        assert nextID == maxID;
        
        long time = System.currentTimeMillis();
        if (idBlockRenewer!=null && idBlockRenewer.isAlive()) {
            //Updating thread has not yet completed, so wait for it
            log.debug("Waiting for id renewal thread");
            idBlockRenewer.join(MAX_WAIT_TIME);
        }
        Preconditions.checkArgument(bufferMaxID>0 && bufferNextID>0,bufferMaxID+" vs. " + bufferNextID);
        
        nextID = bufferNextID;
        maxID = bufferMaxID;
        
        log.debug("[{}] Next/Max ID: {}",partitionID,new long[]{nextID,maxID});

        assert nextID>0 && maxID>nextID;
        
        bufferNextID = -1;
        bufferMaxID = -1;
        
        renewBufferID = maxID - RENEW_ID_COUNT;
        if (renewBufferID>=maxID) renewBufferID = maxID-1;
        if (renewBufferID<nextID) renewBufferID = nextID;
        assert renewBufferID>=nextID && renewBufferID<maxID;
    }
    
    private void renewBuffer() {
        Preconditions.checkArgument(bufferNextID==-1 && bufferMaxID==-1,bufferMaxID+" vs. " + bufferNextID);
        try {
            long[] idblock = idAuthority.getIDBlock(partitionID);
            bufferNextID = idblock[0];
            bufferMaxID = idblock[1];
        } catch (StorageException e) {
            throw new TitanException("Could not acquire new ID block from storage",e);
        }
        Preconditions.checkArgument(bufferNextID>0,bufferNextID);
        Preconditions.checkArgument(bufferMaxID>bufferNextID,bufferMaxID+" vs. " + bufferNextID);
    }

    @Override
    public synchronized long nextID() {
        assert nextID<=maxID;
        if (!initialized) {
            renewBuffer();
            initialized=true;
        }

        if (nextID==maxID) {
            try {
                nextBlock();
            } catch (InterruptedException e) {
                throw new TitanException("Could not renew id block",e);
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
        log.trace("[{}] Returned id: {}",partitionID,returnId);
        return returnId;
    }

    @Override
    public synchronized void close() {
        //TODO: release unused ids, current and buffer
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
