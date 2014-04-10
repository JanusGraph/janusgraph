package com.thinkaurelius.titan.graphdb.database.management;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.log.Log;
import com.thinkaurelius.titan.diskstorage.log.Message;
import com.thinkaurelius.titan.diskstorage.log.MessageReader;
import com.thinkaurelius.titan.diskstorage.util.Timestamps;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.cache.SchemaCache;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanSchemaVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ManagementLogger implements MessageReader {

    private static final Logger log =
            LoggerFactory.getLogger(ManagementLogger.class);

    private static final int SLEEP_INTERVAL_MICRO =  100000;
    private static final int MAX_WAIT_TIME_MICRO = 60000000; //60 seconds


    private final StandardTitanGraph graph;
    private final SchemaCache schemaCache;
    private final Log sysLog;

    private final AtomicInteger evictionTriggerCounter = new AtomicInteger(0);
    private final ConcurrentMap<Long,EvictionTrigger> evictionTriggerMap = new ConcurrentHashMap<Long,EvictionTrigger>();

    public ManagementLogger(StandardTitanGraph graph, Log sysLog, SchemaCache schemaCache) {
        this.graph = graph;
        this.schemaCache = schemaCache;
        this.sysLog = sysLog;
    }

    @Override
    public void read(Message message) {
        ReadBuffer in = message.getContent().asReadBuffer();
        String senderId = message.getSenderId();
        Serializer serializer = graph.getDataSerializer();
        MgmtLogType logType = serializer.readObjectNotNull(in, MgmtLogType.class);
        Preconditions.checkNotNull(logType);
        if (logType==MgmtLogType.CACHED_TYPE_EVICTION) {
                long evictionId = VariableLong.readPositive(in);
                long numEvictions = VariableLong.readPositive(in);
                for (int i = 0; i < numEvictions; i++) {
                    long typeId = VariableLong.readPositive(in);
                    schemaCache.expireTypeRelations(typeId);
                }
                Thread ack = new Thread(new SendAckOnTxClose(evictionId,senderId,graph.getOpenTransactions()));
                ack.setDaemon(true);
                ack.start();
        } else if (logType == MgmtLogType.CACHED_TYPE_EVICTION_ACK) {
                String receiverId = serializer.readObjectNotNull(in,String.class);
                long evictionId = VariableLong.readPositive(in);
                if (receiverId.equals(graph.getConfiguration().getUniqueGraphId())) {
                    //Acknowledgements targeted at this instance
                    EvictionTrigger evictTrigger = evictionTriggerMap.get(evictionId);
                    if (evictTrigger!=null) {
                        evictTrigger.receivedAcknowledgement(senderId);
                    } else log.info("Could not find eviction trigger for {} from {}",evictionId,senderId);
                }

        } else assert logType == MgmtLogType.CONFIG_MUTATION;

    }

    public void sendCacheEviction(Set<TitanSchemaVertex> updatedTypes,
                                             Set<Callable<Boolean>> updatedTypeTriggers,
                                             Set<String> openInstances) {
        Preconditions.checkArgument(!openInstances.isEmpty());
        long evictionId = evictionTriggerCounter.incrementAndGet();
        evictionTriggerMap.put(evictionId,new EvictionTrigger(evictionId,updatedTypeTriggers,openInstances));
        DataOutput out = graph.getDataSerializer().getDataOutput(128);
        out.writeObjectNotNull(MgmtLogType.CACHED_TYPE_EVICTION);
        VariableLong.writePositive(out,evictionId);
        VariableLong.writePositive(out,updatedTypes.size());
        for (TitanSchemaVertex type : updatedTypes) {
            assert type.hasId();
            VariableLong.writePositive(out,type.getID());
        }
        sysLog.add(out.getStaticBuffer());
    }

    private class EvictionTrigger {

        final long evictionId;
        final Set<Callable<Boolean>> updatedTypeTriggers;
        final ImmutableSet<String> openInstances;

        final AtomicInteger ackCounter;
        final long startTime;

        private EvictionTrigger(long evictionId, Set<Callable<Boolean>> updatedTypeTriggers, Set<String> openInstances) {
            this.evictionId = evictionId;
            this.updatedTypeTriggers = updatedTypeTriggers;
            this.openInstances = ImmutableSet.copyOf(openInstances);
            this.ackCounter = new AtomicInteger(openInstances.size());
            this.startTime = Timestamps.MICRO.getTime();
        }

        void receivedAcknowledgement(String senderId) {
            if (openInstances.contains(senderId)) {
                int countdown = ackCounter.decrementAndGet();
                if (countdown==0) { //Trigger actions
                    for (Callable<Boolean> trigger : updatedTypeTriggers) {
                        try {
                            boolean success = trigger.call();
                            assert success;
                        } catch (Throwable e) {
                            log.error("Could not execute trigger ["+trigger.toString()+"] for eviction ["+evictionId+"]",e);
                        }
                    }
                    log.info("Received all acknowledgements for eviction [{}]",evictionId);
                    evictionTriggerMap.remove(evictionId,this);
                }
            }
        }
    }

    private class SendAckOnTxClose implements Runnable {

        private final long evictionId;
        private final Set<? extends TitanTransaction> openTx;
        private final String originId;

        private SendAckOnTxClose(long evictionId, String originId, Set<? extends TitanTransaction> openTx) {
            this.evictionId = evictionId;
            this.openTx = openTx;
            this.originId = originId;
        }

        @Override
        public void run() {
            long startTime = Timestamps.MICRO.getTime();
            while (true) {
                boolean txStillOpen = false;
                Iterator<? extends TitanTransaction> iter = openTx.iterator();
                while (iter.hasNext()) {
                    if (iter.next().isClosed()) {
                        iter.remove();
                    } else {
                        txStillOpen = true;
                    }
                }
                if (!txStillOpen) {
                    //Send ack and finish up
                    DataOutput out = graph.getDataSerializer().getDataOutput(64);
                    out.writeObjectNotNull(MgmtLogType.CACHED_TYPE_EVICTION_ACK);
                    out.writeObjectNotNull(originId);
                    VariableLong.writePositive(out,evictionId);
                    sysLog.add(out.getStaticBuffer());
                    break;
                }
                if (Timestamps.MICRO.getTime()-startTime>MAX_WAIT_TIME_MICRO) {
                    //Break out if waited too long
                    log.error("Evicted [{}] from cache but waiting too long for transactions to close. Stale transaction alert on: {}",getId(),openTx);
                    break;
                }
                try {
                    Timestamps.MICRO.sleepPast(SLEEP_INTERVAL_MICRO, TimeUnit.MICROSECONDS);
                } catch (InterruptedException e) {
                    log.error("Interrupted eviction ack thread for "+getId(),e);
                    break;
                }
            }
        }

        public String getId() {
            return evictionId+"@"+originId;
        }
    }

}
