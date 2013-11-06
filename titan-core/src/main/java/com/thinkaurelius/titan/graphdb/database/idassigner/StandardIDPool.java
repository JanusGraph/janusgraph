package com.thinkaurelius.titan.graphdb.database.idassigner;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.IDAuthority;
import com.thinkaurelius.titan.diskstorage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardIDPool implements IDPool {

    private static final Logger log =
            LoggerFactory.getLogger(StandardIDPool.class);

    private static final long BUFFER_EMPTY = -1;
    private static final long BUFFER_POOL_EXHAUSTION = -100;

    private static final int RENEW_ID_COUNT = 100;

    private static final long RENEW_WAIT_INTERVAL = 1000;


    private final IDAuthority idAuthority;
    private final long maxID; //inclusive
    private final int partitionID;

    private final long renewTimeoutMS;
    private final double renewBufferPercentage;

    private long nextID;
    private long currentMaxID;
    private long renewBufferID;

    private volatile long bufferNextID;
    private volatile long bufferMaxID;
    private Thread idBlockRenewer;

    private boolean initialized;

    public StandardIDPool(IDAuthority idAuthority, long partitionID, long maximumID, long renewTimeoutMS, double renewBufferPercentage) {
        Preconditions.checkArgument(maximumID > 0);
        this.idAuthority = idAuthority;
        Preconditions.checkArgument(partitionID<(1l<<32));
        this.partitionID = (int) partitionID;
        this.maxID = maximumID;
        Preconditions.checkArgument(renewTimeoutMS>0,"Renew-timeout must be positive");
        this.renewTimeoutMS = renewTimeoutMS;
        Preconditions.checkArgument(renewBufferPercentage>0.0 && renewBufferPercentage<=1.0,"Renew-buffer percentage must be in (0.0,1.0]");
        this.renewBufferPercentage = renewBufferPercentage;

        nextID = 0;
        currentMaxID = 0;
        renewBufferID = 0;

        bufferNextID = BUFFER_EMPTY;
        bufferMaxID = BUFFER_EMPTY;
        idBlockRenewer = null;

        initialized = false;
    }

    private void waitForIDRenewer() throws InterruptedException {
        long timeStart = System.currentTimeMillis(); long timeDelta=0;
        while (idBlockRenewer!= null && idBlockRenewer.isAlive() && ((timeDelta=System.currentTimeMillis()-timeStart)<renewTimeoutMS)) {
            //Updating thread has not yet completed, so wait for it
            if (timeDelta<RENEW_WAIT_INTERVAL) {
                log.debug("Waiting for id renewal thread on partition {} [{} ms]",partitionID,timeDelta);
            } else {
                log.warn("Waiting for id renewal thread on partition {} [{} ms]",partitionID,timeDelta);
            }
            idBlockRenewer.join(RENEW_WAIT_INTERVAL);
        }
        if (idBlockRenewer!=null && idBlockRenewer.isAlive())
            throw new TitanException("ID renewal thread on partition ["+partitionID+"] did not complete in time. ["+(System.currentTimeMillis()-timeStart)+" ms]");
    }

    private synchronized void nextBlock() throws InterruptedException {
        assert nextID == currentMaxID;

        waitForIDRenewer();
        if (bufferMaxID == BUFFER_POOL_EXHAUSTION || bufferNextID == BUFFER_POOL_EXHAUSTION)
            throw new IDPoolExhaustedException("Exhausted ID Pool for partition: " + partitionID);

        Preconditions.checkArgument(bufferMaxID > 0, bufferMaxID);
        Preconditions.checkArgument(bufferNextID > 0, bufferNextID);

        nextID = bufferNextID;
        currentMaxID = bufferMaxID;

        log.debug("[{}] Next/Max ID: {}", partitionID, new long[]{nextID, currentMaxID});

        assert nextID > 0 && currentMaxID > nextID;

        bufferNextID = BUFFER_EMPTY;
        bufferMaxID = BUFFER_EMPTY;

        renewBufferID = currentMaxID - Math.max(RENEW_ID_COUNT, Math.round((currentMaxID - nextID)*renewBufferPercentage));
        if (renewBufferID >= currentMaxID) renewBufferID = currentMaxID - 1;
        if (renewBufferID < nextID) renewBufferID = nextID;
        assert renewBufferID >= nextID && renewBufferID < currentMaxID;
    }

    private void renewBuffer() {
        Preconditions.checkArgument(bufferNextID == BUFFER_EMPTY, bufferNextID);
        Preconditions.checkArgument(bufferMaxID == BUFFER_EMPTY, bufferMaxID);
        try {
            long[] idblock = idAuthority.getIDBlock(partitionID);
            bufferNextID = idblock[0];
            bufferMaxID = idblock[1];
            Preconditions.checkArgument(bufferNextID > 0, bufferNextID);
            Preconditions.checkArgument(bufferMaxID > bufferNextID, bufferMaxID);
        } catch (StorageException e) {
            throw new TitanException("Could not acquire new ID block from storage", e);
        } catch (IDPoolExhaustedException e) {
            bufferNextID = BUFFER_POOL_EXHAUSTION;
            bufferMaxID = BUFFER_POOL_EXHAUSTION;
        }
    }

    @Override
    public synchronized long nextID() {
        assert nextID <= currentMaxID;
        if (!initialized) {
            startNextIDAcquisition();
            initialized = true;
        }

        if (nextID == currentMaxID) {
            try {
                nextBlock();
            } catch (InterruptedException e) {
                throw new TitanException("Could not renew id block due to interruption", e);
            }
        }

        if (nextID == renewBufferID) {
            startNextIDAcquisition();
        }
        long returnId = nextID;
        nextID++;
        if (returnId > maxID) throw new IDPoolExhaustedException("Exhausted max id of " + maxID);
        log.trace("[{}] Returned id: {}", partitionID, returnId);
        return returnId;
    }

    @Override
    public synchronized void close() {
        //Wait for renewer to finish
        try {
            waitForIDRenewer();
        } catch (InterruptedException e) {
            throw new TitanException("Interrupted while waiting for id renewer thread to finish", e);
        }
    }

    private void startNextIDAcquisition() {
        Preconditions.checkArgument(idBlockRenewer == null || !idBlockRenewer.isAlive(), idBlockRenewer);
        //Renew buffer
        log.debug("Starting id block renewal thread upon {}", nextID);
        idBlockRenewer = new IDBlockThread();
        idBlockRenewer.start();
    }

    private class IDBlockThread extends Thread {

        @Override
        public void run() {
            renewBuffer();
            log.debug("Finishing id renewal thread");
        }

    }

}
