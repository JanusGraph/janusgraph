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

package org.janusgraph.graphdb.log;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.log.TransactionRecovery;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.MessageReader;
import org.janusgraph.diskstorage.log.ReadMarker;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.log.LogTxMeta;
import org.janusgraph.graphdb.database.log.LogTxStatus;
import org.janusgraph.graphdb.database.log.TransactionLogHeader;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.SchemaSource;
import org.janusgraph.graphdb.types.indextype.IndexTypeWrapper;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.janusgraph.util.system.BackgroundThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    private static final Duration CLEAN_SLEEP_TIME = Duration.ofSeconds(5);
    private static final Duration MIN_TX_LENGTH = Duration.ofSeconds(5);

    private final StandardJanusGraph graph;
    private final Serializer serializer;
    private final TimestampProvider times;
    private final Duration persistenceTime;
    private final Duration readTime = Duration.ofSeconds(1);
    private final AtomicLong txCounter = new AtomicLong(0);
    private final BackgroundCleaner cleaner;
    private final boolean verboseLogging;

    private final AtomicLong successTxCounter = new AtomicLong(0);
    private final AtomicLong failureTxCounter = new AtomicLong(0);

    private final Cache<StandardTransactionId,TxEntry> txCache;


    public StandardTransactionLogProcessor(StandardJanusGraph graph,
                                           Instant startTime) {
        Preconditions.checkArgument(graph != null && graph.isOpen());
        Preconditions.checkArgument(startTime!=null);
        Preconditions.checkArgument(graph.getConfiguration().hasLogTransactions(),
                "Transaction logging must be enabled for recovery to work");
        Duration maxTxLength = graph.getConfiguration().getMaxCommitTime();
        if (maxTxLength.compareTo(MIN_TX_LENGTH)<0) maxTxLength= MIN_TX_LENGTH;
        Preconditions.checkArgument(maxTxLength != null && !maxTxLength.isZero(),
                "Max transaction time cannot be 0");
        this.graph = graph;
        this.serializer = graph.getDataSerializer();
        this.times = graph.getConfiguration().getTimestampProvider();
        final Log txLog = graph.getBackend().getSystemTxLog();
        this.persistenceTime = graph.getConfiguration().getMaxWriteTime();
        this.verboseLogging = graph.getConfiguration().getConfiguration()
                .get(GraphDatabaseConfiguration.VERBOSE_TX_RECOVERY);
        this.txCache = CacheBuilder.newBuilder()
                .concurrencyLevel(2)
                .initialCapacity(100)
                .expireAfterWrite(maxTxLength.toNanos(), TimeUnit.NANOSECONDS)
                .removalListener((RemovalListener<StandardTransactionId, TxEntry>) notification -> {
                    final RemovalCause cause = notification.getCause();
                    Preconditions.checkArgument(cause == RemovalCause.EXPIRED,
                            "Unexpected removal cause [%s] for transaction [%s]", cause, notification.getKey());
                    final TxEntry entry = notification.getValue();
                    if (entry.status == LogTxStatus.SECONDARY_FAILURE || entry.status == LogTxStatus.PRIMARY_SUCCESS) {
                        failureTxCounter.incrementAndGet();
                        fixSecondaryFailure(notification.getKey(), entry);
                    } else {
                        successTxCounter.incrementAndGet();
                    }
                })
                .build();

        ReadMarker start = ReadMarker.fromTime(startTime);
        txLog.registerReader(start,new TxLogMessageReader());

        cleaner = new BackgroundCleaner();
        cleaner.start();
    }

    public long[] getStatistics() {
        return new long[]{successTxCounter.get(),failureTxCounter.get()};
    }

    public synchronized void shutdown() throws JanusGraphException {
        cleaner.close(CLEAN_SLEEP_TIME);
    }

    private void logRecoveryMsg(String message, Object... args) {
        if (logger.isInfoEnabled() || verboseLogging) {
            String msg = String.format(message,args);
            logger.info(msg);
            if (verboseLogging) System.out.println(msg);
        }
    }

    private void fixSecondaryFailure(final StandardTransactionId txId, final TxEntry entry) {
        logRecoveryMsg("Attempting to repair partially failed transaction [%s]",txId);
        if (entry.entry==null) {
            logRecoveryMsg("Trying to repair expired or unpersisted transaction [%s] (Ignore in startup)", txId);
            return;
        }

        boolean userLogFailure = true;
        boolean secIndexFailure = true;
        final Predicate<String> isFailedIndex;
        final TransactionLogHeader.Entry commitEntry = entry.entry;
        final TransactionLogHeader.SecondaryFailures secFail = entry.failures;
        if (secFail!=null) {
            userLogFailure = secFail.userLogFailure;
            secIndexFailure = !secFail.failedIndexes.isEmpty();
            isFailedIndex = secFail.failedIndexes::contains;
        } else {
            isFailedIndex = Predicates.alwaysTrue();
        }

        // I) Restore external indexes
        if (secIndexFailure) {
            restoreExternalIndexes(isFailedIndex, commitEntry);
        }

        // II) Restore log messages
        final String logTxIdentifier = (String)commitEntry.getMetadata().get(LogTxMeta.LOG_ID);
        if (userLogFailure && logTxIdentifier!=null) {
            TransactionLogHeader txHeader = new TransactionLogHeader(txCounter.incrementAndGet(),times.getTime(), times);
            final StaticBuffer userLogContent = txHeader.serializeUserLog(serializer,commitEntry,txId);
            BackendOperation.execute(() -> {
                final Log userLog = graph.getBackend().getUserLog(logTxIdentifier);
                final Future<Message> env = userLog.add(userLogContent);
                if (env.isDone()) {
                    env.get();
                }
                return true;
            }, persistenceTime);
        }
    }



    private void restoreExternalIndexes(Predicate<String> isFailedIndex, TransactionLogHeader.Entry entry) {
        //1) Collect all elements (vertices and relations) and the indexes for which they need to be restored
        SetMultimap<String,IndexRestore> indexRestores = HashMultimap.create();
        BackendOperation.execute(() -> {
            final StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
            try {
                entry.getContentAsModifications(serializer).stream()
                    .map(m -> ModificationDeserializer.parseRelation(m, tx))
                    .forEach(rel -> {
                        //Collect affected vertex indexes
                        for (final MixedIndexType index : getMixedIndexes(rel.getType())) {
                            if (index.getElement()== ElementCategory.VERTEX
                                && isFailedIndex.apply(index.getBackingIndexName())) {
                                assert rel.isProperty();
                                indexRestores.put(index.getBackingIndexName(), new IndexRestore(
                                    rel.getVertex(0).longId(), ElementCategory.VERTEX, getIndexId(index)));
                            }
                        }
                        //See if relation itself is affected
                        for (final RelationType relType : rel.getPropertyKeysDirect()) {
                            for (final MixedIndexType index : getMixedIndexes(relType)) {
                                if (index.getElement().isInstance(rel)
                                    && isFailedIndex.apply(index.getBackingIndexName())) {
                                    assert rel.id() instanceof RelationIdentifier;
                                    indexRestores.put(index.getBackingIndexName(), new IndexRestore(rel.id(),
                                        ElementCategory.getByClazz(rel.getClass()), getIndexId(index)));
                                }
                            }
                        }
                    });
            } finally {
                if (tx.isOpen()) tx.rollback();
            }
            return true;
        }, readTime);


        //2) Restore elements per backing index
        for (final String indexName : indexRestores.keySet()) {
            final StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
            try {
                BackendTransaction btx = tx.getTxHandle();
                final IndexTransaction indexTx = btx.getIndexTransaction(indexName);
                BackendOperation.execute(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        Map<String,Map<String,List<IndexEntry>>> restoredDocs = Maps.newHashMap();
                        indexRestores.get(indexName).forEach(restore -> {
                            JanusGraphSchemaVertex indexV = (JanusGraphSchemaVertex)tx.getVertex(restore.indexId);
                            MixedIndexType index = (MixedIndexType)indexV.asIndexType();
                            JanusGraphElement element = restore.retrieve(tx);
                            if (element!=null) {
                                graph.getIndexSerializer().reindexElement(element,index,restoredDocs);
                            } else { //Element is deleted
                                graph.getIndexSerializer().removeElement(restore.elementId,index,restoredDocs);
                            }
                        });
                        indexTx.restore(restoredDocs);
                        indexTx.commit();
                        return true;
                    }

                    @Override
                    public String toString() {
                        return "IndexMutation";
                    }
                }, persistenceTime);
            } finally {
                if (tx.isOpen()) tx.rollback();
            }
        }
    }

    private static class IndexRestore {

        private final Object elementId;
        private final long indexId;
        private final ElementCategory elementCategory;

        private IndexRestore(Object elementId, ElementCategory category, long indexId) {
            this.elementId = elementId;
            this.indexId = indexId;
            this.elementCategory = category;
        }

        public JanusGraphElement retrieve(JanusGraphTransaction tx) {
            return elementCategory.retrieve(elementId,tx);
        }

        @Override
        public int hashCode() {
            return Objects.hash(elementId, indexId);
        }

        @Override
        public boolean equals(Object other) {
            if (this==other) return true;
            else if (other==null || !getClass().isInstance(other)) return false;
            IndexRestore r = (IndexRestore)other;
            return r.elementId.equals(elementId) && indexId==r.indexId;
        }

    }

    private static long getIndexId(IndexType index) {
        SchemaSource base = ((IndexTypeWrapper)index).getSchemaBase();
        assert base instanceof JanusGraphSchemaVertex;
        return base.longId();
    }

    private static Iterable<MixedIndexType> getMixedIndexes(RelationType type) {
        if (!type.isPropertyKey()) {
            return Collections.emptyList();
        }
        return Iterables.filter(Iterables.filter(((InternalRelationType)type).getKeyIndexes(), MIXED_INDEX_FILTER),
                MixedIndexType.class);
    }

    private static final Predicate<IndexType> MIXED_INDEX_FILTER = IndexType::isMixedIndex;

    private class TxLogMessageReader implements MessageReader {

        private final Callable<TxEntry> entryFactory = TxEntry::new;

        @Override
        public void read(Message message) {
            ReadBuffer content = message.getContent().asReadBuffer();
            String senderId =  message.getSenderId();
            TransactionLogHeader.Entry txentry = TransactionLogHeader.parse(content,serializer,times);
            TransactionLogHeader txheader = txentry.getHeader();
            StandardTransactionId transactionId = new StandardTransactionId(senderId,txheader.getId(),
                    txheader.getTimestamp());

            TxEntry entry;
            try {
                entry = txCache.get(transactionId,entryFactory);
            } catch (ExecutionException e) {
                throw new AssertionError("Unexpected exception",e);
            }

            entry.update(txentry);
        }

        @Override
        public void updateState() {}
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

        private Instant lastInvocation = null;

        public BackgroundCleaner() {
            super("TxLogProcessorCleanup", false);
        }

        @Override
        protected void waitCondition() throws InterruptedException {
            if (lastInvocation!=null) times.sleepPast(lastInvocation.plus(CLEAN_SLEEP_TIME));
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
