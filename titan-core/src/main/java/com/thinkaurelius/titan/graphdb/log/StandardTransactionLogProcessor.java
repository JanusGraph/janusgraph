package com.thinkaurelius.titan.graphdb.log;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.*;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.core.attribute.Timestamp;
import com.thinkaurelius.titan.core.log.TransactionRecovery;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.log.*;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.util.time.StandardTimestamp;
import com.thinkaurelius.titan.diskstorage.util.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.log.LogTxMeta;
import com.thinkaurelius.titan.graphdb.database.log.LogTxStatus;
import com.thinkaurelius.titan.graphdb.database.log.TransactionLogHeader;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.util.system.BackgroundThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardTransactionLogProcessor implements TransactionRecovery {

    private static final Logger logger =
            LoggerFactory.getLogger(StandardTransactionLogProcessor.class);

    private static final Duration CLEAN_SLEEP_TIME = new StandardDuration(5,TimeUnit.SECONDS);
    private static final Duration MIN_TX_LENGTH = new StandardDuration(5, TimeUnit.SECONDS);

    private final StandardTitanGraph graph;
    private final Serializer serializer;
    private final TimestampProvider times;
    private final Log txLog;
    private final Duration persistenceTime;
    private final AtomicLong txCounter = new AtomicLong(0);
    private final BackgroundCleaner cleaner;

    private final AtomicLong successTxCounter = new AtomicLong(0);
    private final AtomicLong failureTxCounter = new AtomicLong(0);

    private final Cache<StandardTransactionId,TxEntry> txCache;


    public StandardTransactionLogProcessor(StandardTitanGraph graph,
                                           Timestamp startTime) {
        Preconditions.checkArgument(graph != null && graph.isOpen());
        Preconditions.checkArgument(startTime!=null);
        Preconditions.checkArgument(graph.getConfiguration().hasLogTransactions(),"Transaction logging must be enabled for recovery to work");
        Duration maxTxLength = graph.getConfiguration().getMaxCommitTime();
        if (maxTxLength.compareTo(MIN_TX_LENGTH)<0) maxTxLength= MIN_TX_LENGTH;
        Preconditions.checkArgument(maxTxLength != null && !maxTxLength.isZeroLength(), "Max transaction time cannot be 0");
        this.graph = graph;
        this.serializer = graph.getDataSerializer();
        this.times = graph.getConfiguration().getTimestampProvider();
        this.txLog = graph.getBackend().getSystemTxLog();
        this.persistenceTime = graph.getConfiguration().getMaxWriteTime();
        this.txCache = CacheBuilder.newBuilder()
                .concurrencyLevel(2)
                .initialCapacity(100)
                .expireAfterWrite(maxTxLength.getLength(maxTxLength.getNativeUnit()), maxTxLength.getNativeUnit())
                .removalListener(new RemovalListener<StandardTransactionId, TxEntry>() {
                    @Override
                    public void onRemoval(RemovalNotification<StandardTransactionId, TxEntry> notification) {
                        RemovalCause cause = notification.getCause();
                        Preconditions.checkArgument(cause == RemovalCause.EXPIRED,
                                "Unexpected removal cause [%s] for transaction [%s]", cause, notification.getKey());
                        TxEntry entry = notification.getValue();
                        if (entry.status == LogTxStatus.SECONDARY_FAILURE || entry.status == LogTxStatus.PRIMARY_SUCCESS) {
                            failureTxCounter.incrementAndGet();
                            fixSecondaryFailure(notification.getKey(), entry);
                        } else {
                            successTxCounter.incrementAndGet();
                        }
                    }
                })
                .build();

        ReadMarker start = ReadMarker.fromTime(startTime.sinceEpoch(startTime.getNativeUnit()),startTime.getNativeUnit());
        this.txLog.registerReader(start,new TxLogMessageReader());

        cleaner = new BackgroundCleaner();
        cleaner.start();
    }

    public long[] getStatistics() {
        return new long[]{successTxCounter.get(),failureTxCounter.get()};
    }

    public synchronized void shutdown() throws TitanException {
        cleaner.close(CLEAN_SLEEP_TIME.getLength(CLEAN_SLEEP_TIME.getNativeUnit()),CLEAN_SLEEP_TIME.getNativeUnit());
        graph.shutdown();
    }

    private void fixSecondaryFailure(final StandardTransactionId txId, final TxEntry entry) {
        if (entry.entry==null) {
            logger.info("Trying to repair expired or unpersisted transaction [{}] (Ignore in startup)",txId);
            return;
        }

        boolean userLogFailure = true;
        boolean secIndexFailure = true;
        Predicate<String> isFailedIndex = Predicates.alwaysTrue();
        final TransactionLogHeader.Entry commitEntry = entry.entry;
        final TransactionLogHeader.SecondaryFailures secFail = entry.failures;
        if (secFail!=null) {
            userLogFailure = secFail.userLogFailure;
            secIndexFailure = !secFail.failedIndexes.isEmpty();
            isFailedIndex = new Predicate<String>() {
                @Override
                public boolean apply(@Nullable String s) {
                    return secFail.failedIndexes.contains(s);
                }
            };
        }

        if (secIndexFailure) {
            //TODO
        }

        final String logTxIdentifier = (String)commitEntry.getMetadata().get(LogTxMeta.LOG_ID);
        if (userLogFailure && logTxIdentifier!=null) {
            TransactionLogHeader txHeader = new TransactionLogHeader(txCounter.incrementAndGet(),times.getTime());
            final StaticBuffer userLogContent = txHeader.serializeUserLog(serializer,commitEntry,txId);
            BackendOperation.execute(new Callable<Boolean>(){
                @Override
                public Boolean call() throws Exception {
                    final Log triggerLog = graph.getBackend().getTriggerLog(logTxIdentifier);
                    Future<Message> env = triggerLog.add(userLogContent);
                    if (env.isDone()) {
                        env.get();
                    }
                    return true;
                }
            },persistenceTime);
        }


    }

//    private void readRelations(TransactionLogHeader.Entry txentry,
//                               StandardTitanTx tx, StandardChangeState changes) {
//        for (TransactionLogHeader.Modification modification : txentry.getContentAsModifications(serializer)) {
//            Change state = modification.state;
//            assert state.isProper();
//            long outVertexId = modification.outVertexId;
//            Entry relEntry = modification.relationEntry;
//            InternalVertex outVertex = tx.getInternalVertex(outVertexId);
//            //Special relation parsing, compare to {@link RelationConstructor}
//            RelationCache relCache = tx.getEdgeSerializer().readRelation(relEntry, false, tx);
//            assert relCache.direction == Direction.OUT;
//            InternalRelationType type = (InternalRelationType)tx.getExistingRelationType(relCache.typeId);
//            assert type.getBaseType()==null;
//            InternalRelation rel;
//            if (type.isPropertyKey()) {
//                if (state==Change.REMOVED) {
//                    rel = new StandardProperty(relCache.relationId,(PropertyKey)type,outVertex,relCache.getValue(), ElementLifeCycle.Removed);
//                } else {
//                    rel = new CacheProperty(relCache.relationId,(PropertyKey)type,outVertex,relCache.getValue(),relEntry);
//                }
//            } else {
//                assert type.isEdgeLabel();
//                InternalVertex otherVertex = tx.getInternalVertex(relCache.getOtherVertexId());
//                if (state==Change.REMOVED) {
//                    rel = new StandardEdge(relCache.relationId, (EdgeLabel) type, outVertex, otherVertex,ElementLifeCycle.Removed);
//                } else {
//                    rel = new CacheEdge(relCache.relationId, (EdgeLabel) type, outVertex, otherVertex,relEntry);
//                }
//            }
//            if (state==Change.REMOVED && relCache.hasProperties()) { //copy over properties
//                for (LongObjectCursor<Object> entry : relCache) {
//                    rel.setPropertyDirect(tx.getExistingRelationType(entry.key),entry.value);
//                }
//            }
//
//            //Special case for vertex addition/removal
//            if (rel.getType().equals(BaseKey.VertexExists) && !(outVertex instanceof TitanSchemaElement)) {
//                if (state==Change.REMOVED) { //Mark as removed
//                    ((StandardVertex)outVertex).updateLifeCycle(ElementLifeCycle.Event.REMOVED);
//                }
//                changes.addVertex(outVertex,state);
//            } else if (!rel.isHidden()) {
//                changes.addRelation(rel,state);
//            }
//        }
//    }

    private class TxLogMessageReader implements MessageReader {

        private final Callable<TxEntry> entryFactory = new Callable<TxEntry>() {
            @Override
            public TxEntry call() throws Exception {
                return new TxEntry();
            }
        };

        @Override
        public void read(Message message) {
            ReadBuffer content = message.getContent().asReadBuffer();
            String senderId =  message.getSenderId();
            TransactionLogHeader.Entry txentry = TransactionLogHeader.parse(content,serializer,times);
            TransactionLogHeader txheader = txentry.getHeader();
            StandardTransactionId transactionId = new StandardTransactionId(senderId,txheader.getId(),
                    new StandardTimestamp(txheader.getTimestamp(times.getUnit()),times.getUnit()));

            TxEntry entry;
            try {
                entry = txCache.get(transactionId,entryFactory);
            } catch (ExecutionException e) {
                throw new AssertionError("Unexpected exception",e);
            }

            entry.update(txentry);
        }


    }

    private class TxEntry {

        LogTxStatus status;
        TransactionLogHeader.Entry entry;
        TransactionLogHeader.SecondaryFailures failures;

        synchronized void update(TransactionLogHeader.Entry e) {
            switch (e.getStatus()) {
                case PRECOMMIT:
                    entry = e;
                    if (status==null) status=LogTxStatus.PRECOMMIT;
                    break;
                case PRIMARY_SUCCESS:
                    if (status==null || status==LogTxStatus.PRECOMMIT) status=LogTxStatus.PRIMARY_SUCCESS;
                    break;
                case COMPLETE_SUCCESS:
                    if (status==null || status==LogTxStatus.PRECOMMIT) status=LogTxStatus.COMPLETE_SUCCESS;
                    break;
                case SECONDARY_SUCCESS:
                    status=LogTxStatus.SECONDARY_SUCCESS;
                    break;
                case SECONDARY_FAILURE:
                    status=LogTxStatus.SECONDARY_FAILURE;
                    failures=e.getContentAsSecondaryFailures(serializer);
                    break;
                default: throw new AssertionError("Unexpected status: " + e.getStatus());
            }
        }

    }

    private class BackgroundCleaner extends BackgroundThread {

        private Timepoint lastInvocation = null;

        public BackgroundCleaner() {
            super("TxLogProcessorCleanup", false);
        }

        @Override
        protected void waitCondition() throws InterruptedException {
            if (lastInvocation!=null) times.sleepPast(lastInvocation.add(CLEAN_SLEEP_TIME));
        }

        @Override
        protected void action() {
            lastInvocation = times.getTime();
            txCache.cleanUp();
        }

        @Override
        protected void cleanup() {
            txCache.cleanUp();
        }
    }

}
