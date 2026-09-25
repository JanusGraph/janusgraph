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
import org.apache.commons.collections.CollectionUtils;
import org.janusgraph.core.JanusGraphManagerUtility;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.ResourceUnavailableException;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.MessageReader;
import org.janusgraph.diskstorage.util.time.Timer;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.cache.SchemaCache;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.janusgraph.graphdb.database.management.GraphCacheEvictionAction.DO_NOT_EVICT;
import static org.janusgraph.graphdb.database.management.GraphCacheEvictionAction.EVICT;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ManagementLogger implements MessageReader {

    private static final Logger log =
            LoggerFactory.getLogger(ManagementLogger.class);

    private static final Duration SLEEP_INTERVAL = Duration.ofMillis(100L);
    private static final Duration MAX_WAIT_TIME = Duration.ofSeconds(60L);
    //The id of an eviction which nobody waits for. Acknowledged evictions take theirs from nextEvictionId(), never this
    private static final long UNACKNOWLEDGED_EVICTION_ID = 0;

    private final StandardJanusGraph graph;
    private final SchemaCache schemaCache;
    private final Log sysLog;

    /**
     * This belongs in JanusGraphConfig.
     */
    private final TimestampProvider times;

    private final AtomicLong evictionTriggerCounter = new AtomicLong(0);
    private final ConcurrentMap<Long,EvictionTrigger> evictionTriggerMap = new ConcurrentHashMap<>();

    private final Duration ackTimeout;
    private final boolean autoCloseStaleInstances;

    public ManagementLogger(StandardJanusGraph graph, Log sysLog, SchemaCache schemaCache, TimestampProvider times) {
        this(graph, sysLog, schemaCache, times, Duration.ofSeconds(120), true);
    }

    public ManagementLogger(StandardJanusGraph graph, Log sysLog, SchemaCache schemaCache, TimestampProvider times,
                            Duration ackTimeout, boolean autoCloseStaleInstances) {
        this.graph = graph;
        this.schemaCache = schemaCache;
        this.sysLog = sysLog;
        this.times = times;
        this.ackTimeout = ackTimeout;
        this.autoCloseStaleInstances = autoCloseStaleInstances;
        Preconditions.checkNotNull(times);
    }

    @Override
    public void read(Message message) {
        ReadBuffer in = message.getContent().asReadBuffer();
        String senderId = message.getSenderId();
        Serializer serializer = graph.getDataSerializer();
        MgmtLogType logType = serializer.readObjectNotNull(in, MgmtLogType.class);
        Preconditions.checkNotNull(logType);
        switch (logType) {
            case CACHED_TYPE_EVICTION: {
                long evictionId = VariableLong.readPositive(in);
                long numEvictions = VariableLong.readPositive(in);
                for (int i = 0; i < numEvictions; i++) {
                    long typeId = VariableLong.readPositive(in);
                    schemaCache.expireSchemaElement(typeId);
                    for (JanusGraphTransaction tx : graph.getOpenTransactions()) {
                        //Expiring an element reloads its definition, which can fail like any read; the other elements
                        //and transactions of the message must not be left stale for it
                        try {
                            tx.expireSchemaElement(typeId);
                        } catch (RuntimeException e) {
                            log.warn("Could not expire schema element {} in transaction {}; it is reloaded when the "
                                + "transaction next uses it", typeId, tx, e);
                        }
                    }
                }
                final GraphCacheEvictionAction action = serializer.readObjectNotNull(in, GraphCacheEvictionAction.class);
                Preconditions.checkNotNull(action);
                if (evictionId == UNACKNOWLEDGED_EVICTION_ID) {
                    //Nobody waits for an acknowledgement of this one
                    break;
                }
                final Thread ack = new Thread(new SendAckOnTxClose(evictionId, senderId, graph.getOpenTransactions(), action, graph.getGraphName()));
                ack.setDaemon(true);
                ack.start();
                break;
            }
            case CACHED_TYPE_EVICTION_ACK: {
                String receiverId = serializer.readObjectNotNull(in, String.class);
                long evictionId = VariableLong.readPositive(in);
                if (evictionId == UNACKNOWLEDGED_EVICTION_ID) {
                    //Instances of earlier versions acknowledge every eviction, this kind included: nothing waits for it
                    break;
                }
                if (receiverId.equals(graph.getConfiguration().getUniqueGraphId())) {
                    //Acknowledgements targeted at this instance
                    EvictionTrigger evictTrigger = evictionTriggerMap.get(evictionId);
                    if (evictTrigger != null) {
                        evictTrigger.receivedAcknowledgement(senderId);
                    } else log.error("Could not find eviction trigger for {} from {}", evictionId, senderId);
                }

                break;
            }
            default:
                assert logType == MgmtLogType.CONFIG_MUTATION;
                break;
        }

    }

    //From 1 up, never UNACKNOWLEDGED_EVICTION_ID: the counter starts over at 1 rather than go negative, as the int
    //counter before it did after overflowing, and a long takes 2^63 evictions to get there, so an id is never handed
    //out again while the trigger it went to may still be waiting for acknowledgements
    private long nextEvictionId() {
        return evictionTriggerCounter.updateAndGet(id -> id == Long.MAX_VALUE ? 1 : id + 1);
    }

    public void sendCacheEviction(Set<JanusGraphSchemaVertex> updatedTypes,
                                             final boolean evictGraphFromCache,
                                             List<Callable<Boolean>> updatedTypeTriggers,
                                             Set<String> openInstances) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(openInstances), "openInstances cannot be null or empty");
        long evictionId = nextEvictionId();
        evictionTriggerMap.put(evictionId,new EvictionTrigger(evictionId,updatedTypeTriggers,graph));
        DataOutput out = graph.getDataSerializer().getDataOutput(128);
        out.writeObjectNotNull(MgmtLogType.CACHED_TYPE_EVICTION);
        VariableLong.writePositive(out,evictionId);
        VariableLong.writePositive(out,updatedTypes.size());
        for (JanusGraphSchemaVertex type : updatedTypes) {
            assert type.hasId();
            VariableLong.writePositive(out,type.longId());
        }
        if (evictGraphFromCache) {
            out.writeObjectNotNull(EVICT);
        } else {
            out.writeObjectNotNull(DO_NOT_EVICT);
        }
        sysLog.add(out.getStaticBuffer());
    }

    /**
     * Tells every instance, this one included, to expire the given schema elements from its schema cache and open
     * transactions, without anything waiting for acknowledgements. This is for the definition edges no management
     * eviction covers: constraints auto-created under schema.constraints=true, and connections and properties added
     * through addConnection and addProperties. An acknowledged eviction would register an eviction trigger for every
     * such commit, which opens a management transaction to list the open instances and waits for each of them to
     * acknowledge once its open transactions have closed, and with graph.management-auto-close-stale-instances it
     * could get an instance with a long-running transaction force-closed for an ordinary write. The message is the one
     * {@link #sendCacheEviction} sends, with the elements to expire and never an eviction of the graph itself, so
     * instances of earlier versions process it as well; they acknowledge it, and the acknowledgement is ignored.
     *
     * @param schemaIds the ids of the schema elements to expire
     */
    public void sendUnacknowledgedCacheEviction(Collection<Long> schemaIds) {
        Preconditions.checkArgument(!schemaIds.isEmpty(), "Nothing to evict");
        DataOutput out = graph.getDataSerializer().getDataOutput(64);
        out.writeObjectNotNull(MgmtLogType.CACHED_TYPE_EVICTION);
        VariableLong.writePositive(out, UNACKNOWLEDGED_EVICTION_ID);
        VariableLong.writePositive(out, schemaIds.size());
        for (long schemaId : schemaIds) {
            VariableLong.writePositive(out, schemaId);
        }
        out.writeObjectNotNull(DO_NOT_EVICT);
        sysLog.add(out.getStaticBuffer());
    }

    @Override
    public void updateState() {
        evictionTriggerMap.forEach((k, v) -> {
            final int ackCounter = v.removeDroppedInstances();
            if (ackCounter == 0) {
                v.runTriggers();
            }
        });
    }

    private class EvictionTrigger {

        final long evictionId;
        final List<Callable<Boolean>> updatedTypeTriggers;
        final StandardJanusGraph graph;
        final Set<String> instancesToBeAcknowledged;
        final long createdAtMillis;

        private EvictionTrigger(long evictionId, List<Callable<Boolean>> updatedTypeTriggers, StandardJanusGraph graph) {
            this.graph = graph;
            this.evictionId = evictionId;
            this.updatedTypeTriggers = updatedTypeTriggers;
            this.createdAtMillis = System.currentTimeMillis();
            final JanusGraphManagement mgmt = graph.openManagement();
            this.instancesToBeAcknowledged = ConcurrentHashMap.newKeySet();
            instancesToBeAcknowledged.addAll(((ManagementSystem) mgmt).getOpenInstancesInternal());
            mgmt.rollback();
        }

        void receivedAcknowledgement(String senderId) {
            if (instancesToBeAcknowledged.remove(senderId)) {
                final int ackCounter = instancesToBeAcknowledged.size();
                log.debug("Received acknowledgement for eviction [{}] from senderID={} ({} more acks still outstanding)",
                        evictionId, senderId, ackCounter);
                if (ackCounter == 0) {
                    runTriggers();
                }
            }
        }

        void runTriggers() {
            for (Callable<Boolean> trigger : updatedTypeTriggers) {
                try {
                    final boolean success = trigger.call();
                    assert success;
                } catch (Throwable e) {
                    log.error("Could not execute trigger ["+trigger.toString()+"] for eviction ["+evictionId+"]",e);
                }
            }
            log.info("Received all acknowledgements for eviction [{}]",evictionId);
            evictionTriggerMap.remove(evictionId,this);
        }

        int removeDroppedInstances() {
            final JanusGraphManagement mgmt = graph.openManagement();
            final Set<String> updatedInstances = ((ManagementSystem) mgmt).getOpenInstancesInternal();
            final String instanceRemovedMsg = "Instance [{}] was removed list of open instances and therefore dropped from list of instances to be acknowledged.";
            instancesToBeAcknowledged.stream().filter(it -> !updatedInstances.contains(it)).filter(instancesToBeAcknowledged::remove).forEach(it -> log.debug(instanceRemovedMsg, it));
            mgmt.rollback();

            if (autoCloseStaleInstances && !instancesToBeAcknowledged.isEmpty()
                    && (System.currentTimeMillis() - createdAtMillis) > ackTimeout.toMillis()) {
                log.warn("ACK timeout ({}s) exceeded for eviction [{}]. Force-closing unresponsive instances: {}",
                        ackTimeout.getSeconds(), evictionId, instancesToBeAcknowledged);
                forceCloseStaleInstances(new HashSet<>(instancesToBeAcknowledged));
                instancesToBeAcknowledged.clear();
            }

            return instancesToBeAcknowledged.size();
        }

        private void forceCloseStaleInstances(Set<String> staleInstances) {
            try {
                final JanusGraphManagement mgmt = graph.openManagement();
                for (String instanceId : staleInstances) {
                    try {
                        mgmt.forceCloseInstance(instanceId);
                        log.warn("Force-closed stale instance [{}]", instanceId);
                    } catch (IllegalArgumentException e) {
                        log.debug("Could not force-close instance [{}]: {}", instanceId, e.getMessage());
                    }
                }
                mgmt.commit();
            } catch (Exception e) {
                log.error("Failed to force-close stale instances", e);
            }
        }
    }

    private class SendAckOnTxClose implements Runnable {

        private final long evictionId;
        private final Set<? extends JanusGraphTransaction> openTx;
        private final String originId;
        private final GraphCacheEvictionAction action;
        private final String graphName;

        private SendAckOnTxClose(long evictionId,
                                 String originId,
                                 Set<? extends JanusGraphTransaction> openTx,
                                 GraphCacheEvictionAction action,
                                 String graphName) {
            this.evictionId = evictionId;
            this.openTx = openTx;
            this.originId = originId;
            this.action = action;
            this.graphName = graphName;
        }

        @Override
        public void run() {
//            long startTime = Timestamps.MICRO.getTime();
            Timer t = times.getTimer().start();
            while (true) {
                boolean txStillOpen = false;
                Iterator<? extends JanusGraphTransaction> iterator = openTx.iterator();
                while (iterator.hasNext()) {
                    if (iterator.next().isClosed()) {
                        iterator.remove();
                    } else {
                        txStillOpen = true;
                    }
                }
                final JanusGraphManager jgm = JanusGraphManagerUtility.getInstance();
                final boolean janusGraphManagerIsInBadState = null == jgm && action.equals(EVICT);
                if (!txStillOpen && janusGraphManagerIsInBadState) {
                    log.error("JanusGraphManager should be instantiated on this server, but it is not. " +
                              "Please restart with proper server settings. " +
                              "As a result, we could not evict graph {} from the cache.", graphName);
                    break;
                }
                else if (!txStillOpen) {
                    //Send ack and finish up
                    DataOutput out = graph.getDataSerializer().getDataOutput(64);
                    out.writeObjectNotNull(MgmtLogType.CACHED_TYPE_EVICTION_ACK);
                    out.writeObjectNotNull(originId);
                    VariableLong.writePositive(out,evictionId);
                    if (null != jgm && action.equals(EVICT)) {
                        jgm.removeGraph(graphName);
                        log.debug("Graph {} has been removed from the JanusGraphManager graph cache.", graphName);
                    }
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
