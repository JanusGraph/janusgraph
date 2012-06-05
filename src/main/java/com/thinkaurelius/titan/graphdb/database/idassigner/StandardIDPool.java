package com.thinkaurelius.titan.graphdb.database.idassigner;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.GraphDatabaseException;
import com.thinkaurelius.titan.diskstorage.IDAuthority;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class StandardIDPool implements IDPool {
    
    private static final int RENEW_ID_COUNT = 100;
    
    private static final int SLEEP_TIME = 100;
    private static final int MAX_WAIT_TIME = 2000;
    
    private final IDAuthority idAuthority;
    //private final int blockSize;
    private final int partitionID;
        
    private long nextID;
    private long maxID;
    private long renewBufferID;
    
    private long bufferNextID;
    private long bufferMaxID;
    
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
        
        renewBuffer();
    }
    
    private synchronized void nextBlock() throws InterruptedException {
        assert nextID == maxID;
        
        long time = System.currentTimeMillis();
        while (bufferMaxID<0 || bufferMaxID<0) {
            //Updating thread has not yet completed
            Thread.sleep(SLEEP_TIME);
            if (System.currentTimeMillis()-time>MAX_WAIT_TIME) {
                throw new InterruptedException("Wait time exceeded while allocating new id block");
            }
        }
        
        nextID = bufferNextID;
        maxID = bufferMaxID;

        assert nextID>0 && maxID>nextID;
        
        bufferNextID = -1;
        bufferMaxID = -1;
        
        renewBufferID = maxID - RENEW_ID_COUNT;
        if (renewBufferID>=maxID) renewBufferID = maxID-1;
        if (renewBufferID<nextID) renewBufferID = nextID;
        assert renewBufferID>=nextID && renewBufferID<maxID;
    }
    
    private void renewBuffer() {
        assert bufferNextID==-1 && bufferMaxID==-1;
        long[] idblock = idAuthority.getIDBlock(partitionID);
        bufferNextID = idblock[0];
        bufferMaxID = idblock[1];
        Preconditions.checkArgument(bufferNextID>0);
        Preconditions.checkArgument(bufferMaxID>bufferNextID);
    }

    @Override
    public synchronized long nextID() {
        assert nextID<=maxID;
        if (nextID==maxID) {
            try {
                nextBlock();
            } catch (InterruptedException e) {
                throw new GraphDatabaseException("Could not renew id block",e);
            }
        }

        if (nextID == renewBufferID) {
            //Renew buffer
            new IDBlockThread().start();
        }
        long id = nextID;
        nextID++;
        return id;
    }

    @Override
    public synchronized void close() {
        //TODO: release unused ids, current and buffer
    }

    private class IDBlockThread extends Thread {

        @Override
        public void run() {
            renewBuffer();
        }

    }

}
