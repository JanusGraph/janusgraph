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

package org.janusgraph.graphdb.database.management;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.diskstorage.ResourceUnavailableException;

import org.janusgraph.diskstorage.util.time.Timer;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.MessageReader;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.cache.SchemaCache;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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

    private static final Duration SLEEP_INTERVAL = Duration.ofMillis(100L);
    private static final Duration MAX_WAIT_TIME = Duration.ofSeconds(60L);

    private final StandardJanusGraph graph;
    private final SchemaCache schemaCache;
    private final Log sysLog;

    /**
     * This belongs in JanusGraphConfig.
     */
    private final TimestampProvider times;

    private final AtomicInteger evictionTriggerCounter = new AtomicInteger(0);
    private final ConcurrentMap<Long,EvictionTrigger> evictionTriggerMap = new ConcurrentHashMap<Long,EvictionTrigger>();

    public ManagementLogger(StandardJanusGraph graph, Log sysLog, SchemaCache schemaCache, TimestampProvider times) {
        this.graph = graph;
        this.schemaCache = schemaCache;
        this.sysLog = sysLog;
        this.times = times;
        Preconditions.checkNotNull(times);
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
                    schemaCache.expireSchemaElement(typeId);
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
                    } else log.error("Could not find eviction trigger for {} from {}",evictionId,senderId);
                }

        } else assert logType == MgmtLogType.CONFIG_MUTATION;

    }

    public void sendCacheEviction(Set<JanusGraphSchemaVertex> updatedTypes,
                                             Set<Callable<Boolean>> updatedTypeTriggers,
                                             Set<String> openInstances) {
        Preconditions.checkArgument(!openInstances.isEmpty());
        long evictionId = evictionTriggerCounter.incrementAndGet();
        evictionTriggerMap.put(evictionId,new EvictionTrigger(evictionId,updatedTypeTriggers,openInstances));
        DataOutput out = graph.getDataSerializer().getDataOutput(128);
        out.writeObjectNotNull(MgmtLogType.CACHED_TYPE_EVICTION);
        VariableLong.writePositive(out,evictionId);
        VariableLong.writePositive(out,updatedTypes.size());
        for (JanusGraphSchemaVertex type : updatedTypes) {
            assert type.hasId();
            VariableLong.writePositive(out,type.longId());
        }
        sysLog.add(out.getStaticBuffer());
    }

    private class EvictionTrigger {

        final long evictionId;
        final Set<Callable<Boolean>> updatedTypeTriggers;
        final ImmutableSet<String> openInstances;
        final AtomicInteger ackCounter;

        private EvictionTrigger(long evictionId, Set<Callable<Boolean>> updatedTypeTriggers, Set<String> openInstances) {
            this.evictionId = evictionId;
            this.updatedTypeTriggers = updatedTypeTriggers;
            this.openInstances = ImmutableSet.copyOf(openInstances);
            this.ackCounter = new AtomicInteger(openInstances.size());
        }

        void receivedAcknowledgement(String senderId) {
            if (openInstances.contains(senderId)) {
                int countdown = ackCounter.decrementAndGet();
                log.debug("Received acknowledgement for eviction [{}] from senderID={} ({} more acks still outstanding)",
                        evictionId, senderId, countdown);
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
        private final Set<? extends JanusGraphTransaction> openTx;
        private final String originId;

        private SendAckOnTxClose(long evictionId, String originId, Set<? extends JanusGraphTransaction> openTx) {
            this.evictionId = evictionId;
            this.openTx = openTx;
            this.originId = originId;
        }

        @Override
        public void run() {
//            long startTime = Timestamps.MICRO.getTime();
            Timer t = times.getTimer().start();
            while (true) {
                boolean txStillOpen = false;
                Iterator<? extends JanusGraphTransaction> iter = openTx.iterator();
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
                    try {
                        sysLog.add(out.getStaticBuffer());
                        log.debug("Sent {}: evictionID={} originID={}", MgmtLogType.CACHED_TYPE_EVICTION_ACK, evictionId, originId);
                    } catch (ResourceUnavailableException e) {
                        //During shutdown, this event may be triggered but the log is already closed. The failure to send the acknowledgement
                        //can then be ignored
                        log.warn("System log has already shut down. Did not sent {}: evictionID={} originID={}",MgmtLogType.CACHED_TYPE_EVICTION_ACK,evictionId,originId);
                    }
                    break;
                }
                if (MAX_WAIT_TIME.compareTo(t.elapsed()) < 0) {
                    //Break out if waited too long
                    log.error("Evicted [{}] from cache but waiting too long for transactions to close. Stale transaction alert on: {}",getId(),openTx);
                    break;
                }
                try {
                    times.sleepPast(times.getTime().plus(SLEEP_INTERVAL));
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
