package com.thinkaurelius.titan.graphdb.log;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.*;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.core.attribute.Timestamp;
import com.thinkaurelius.titan.core.log.TransactionRecovery;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.indexing.IndexEntry;
import com.thinkaurelius.titan.diskstorage.indexing.IndexTransaction;
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
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.graphdb.types.MixedIndexType;
import com.thinkaurelius.titan.graphdb.types.SchemaSource;
import com.thinkaurelius.titan.graphdb.types.indextype.IndexTypeWrapper;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanSchemaVertex;
import com.thinkaurelius.titan.util.system.BackgroundThread;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    private final Duration readTime = new StandardDuration(1,TimeUnit.SECONDS);
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
    }

    private void fixSecondaryFailure(final StandardTransactionId txId, final TxEntry entry) {
        if (entry.entry==null) {
            logger.info("Trying to repair expired or unpersisted transaction [{}] (Ignore in startup)",txId);
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
            isFailedIndex = new Predicate<String>() {
                @Override
                public boolean apply(@Nullable String s) {
                    return secFail.failedIndexes.contains(s);
                }
            };
        } else {
            isFailedIndex = Predicates.alwaysTrue();
        }

        if (secIndexFailure) {
            //1) Collect all elements (vertices and relations) and the indexes for which they need to be restored
            final SetMultimap<String,IndexRestore> indexRestores = HashMultimap.create();
            BackendOperation.execute(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    StandardTitanTx tx = (StandardTitanTx) graph.newTransaction();
                    try {
                        for (TransactionLogHeader.Modification modification : commitEntry.getContentAsModifications(serializer)) {
                            InternalRelation rel = ModificationDeserializer.parseRelation(modification,tx);
                            //Collect affected vertex indexes
                            for (MixedIndexType index : getMixedIndexes(rel.getType())) {
                                if (index.getElement()==ElementCategory.VERTEX && isFailedIndex.apply(index.getBackingIndexName())) {
                                    assert rel.isProperty();
                                    indexRestores.put(index.getBackingIndexName(),
                                            new IndexRestore(rel.getVertex(0).getLongId(),ElementCategory.VERTEX,getIndexId(index)));
                                }
                            }
                            //See if relation itself is affected
                            for (RelationType relType : rel.getPropertyKeysDirect()) {
                                for (MixedIndexType index : getMixedIndexes(relType)) {
                                    if (index.getElement().isInstance(rel) && isFailedIndex.apply(index.getBackingIndexName())) {
                                        assert rel.getId() instanceof RelationIdentifier;
                                        indexRestores.put(index.getBackingIndexName(),
                                                new IndexRestore(rel.getId(),ElementCategory.getByClazz(rel.getClass()),getIndexId(index)));
                                    }
                                }
                            }
                        }
                    } finally {
                        if (tx.isOpen()) tx.rollback();
                    }
                    return true;
                }
            },readTime);


            //2) Restore elements per backing index
            for (final String indexName : indexRestores.keySet()) {
                final StandardTitanTx tx = (StandardTitanTx) graph.newTransaction();
                try {
                    BackendTransaction btx = tx.getTxHandle();
                    final IndexTransaction indexTx = btx.getIndexTransaction(indexName);
                    BackendOperation.execute(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            Map<String,Map<String,List<IndexEntry>>> restoredDocs = Maps.newHashMap();
                            for (IndexRestore restore : indexRestores.get(indexName)) {
                                TitanSchemaVertex indexV = (TitanSchemaVertex)tx.getVertex(restore.indexId);
                                MixedIndexType index = (MixedIndexType)indexV.asIndexType();
                                TitanElement element = restore.retrieve(tx);
                                if (element!=null) {
                                    graph.getIndexSerializer().reindexElement(element,index,restoredDocs);
                                } else { //Element is deleted
                                    graph.getIndexSerializer().removeElement(restore.elementId,index,restoredDocs);
                                }
                            }
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

        final String logTxIdentifier = (String)commitEntry.getMetadata().get(LogTxMeta.LOG_ID);
        if (userLogFailure && logTxIdentifier!=null) {
            TransactionLogHeader txHeader = new TransactionLogHeader(txCounter.incrementAndGet(),times.getTime());
            final StaticBuffer userLogContent = txHeader.serializeUserLog(serializer,commitEntry,txId);
            BackendOperation.execute(new Callable<Boolean>(){
                @Override
                public Boolean call() throws Exception {
                    final Log userLog = graph.getBackend().getUserLog(logTxIdentifier);
                    Future<Message> env = userLog.add(userLogContent);
                    if (env.isDone()) {
                        env.get();
                    }
                    return true;
                }
            },persistenceTime);
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

        public TitanElement retrieve(TitanTransaction tx) {
            return elementCategory.retrieve(elementId,tx);
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(elementId).append(indexId).toHashCode();
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
        assert base instanceof TitanSchemaVertex;
        return base.getLongId();
    }

    private static Iterable<MixedIndexType> getMixedIndexes(RelationType type) {
        if (!type.isPropertyKey()) return Collections.EMPTY_LIST;
        return Iterables.filter(Iterables.filter(((InternalRelationType)type).getKeyIndexes(),MIXED_INDEX_FILTER),MixedIndexType.class);
    }

    private static final Predicate<IndexType> MIXED_INDEX_FILTER = new Predicate<IndexType>() {
        @Override
        public boolean apply(@Nullable IndexType indexType) {
            return indexType.isMixedIndex();
        }
    };

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
