package com.thinkaurelius.titan.graphdb.log;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.attribute.Timestamp;
import com.thinkaurelius.titan.core.log.LogProcessorBuilder;
import com.thinkaurelius.titan.core.log.LogProcessorFramework;
import com.thinkaurelius.titan.core.schema.TitanSchemaElement;
import com.thinkaurelius.titan.core.log.Change;
import com.thinkaurelius.titan.core.log.ChangeProcessor;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.log.*;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.time.StandardTimestamp;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.relations.*;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import com.thinkaurelius.titan.graphdb.vertices.StandardVertex;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardLogProcessorFramework implements LogProcessorFramework {

    private static final Logger logger =
            LoggerFactory.getLogger(StandardLogProcessorFramework.class);

    private final StandardTitanGraph graph;
    private final TimestampProvider times;
    private final LogManager triggerManager;
    private final Map<String,Log> processorLogs;

    private boolean isOpen = true;

    public StandardLogProcessorFramework(StandardTitanGraph graph) {
        Preconditions.checkArgument(graph!=null && graph.isOpen());
        this.graph = graph;
        this.times = graph.getConfiguration().getTimestampProvider();
        this.triggerManager = graph.getBackend().getLogManager(GraphDatabaseConfiguration.TRIGGER_LOG);
        this.processorLogs = new HashMap<String, Log>();
    }

    private void checkOpen() {
        Preconditions.checkState(isOpen, "Trigger framework has already been closed");
    }

    @Override
    public synchronized boolean removeLogProcessor(String logIdentifier) {
        checkOpen();
        if (processorLogs.containsKey(logIdentifier)) {
            try {
                processorLogs.get(logIdentifier).close();
            } catch (StorageException e) {
                throw new TitanException("Could not close trigger log: "+ logIdentifier,e);
            }
            processorLogs.remove(logIdentifier);
            return true;
        } else return false;
    }

    @Override
    public synchronized void shutdown() throws TitanException {
        isOpen = false;
        try {
            try {
                for (Log log : processorLogs.values()) {
                    log.close();
                }
                processorLogs.clear();
            } finally {
                triggerManager.close();
            }
        } catch (StorageException e) {
            throw new TitanException(e);
        }
        graph.shutdown();
    }


    @Override
    public LogProcessorBuilder addLogProcessor(String logIdentifier) {
        return new Builder(logIdentifier);
    }

    private class Builder implements LogProcessorBuilder {

        private final String triggerName;
        private final List<ChangeProcessor> processors;

        private String readMarkerName = null;
        private Timestamp startTime = null;
        private int retryAttempts = 1;


        private Builder(String triggerName) {
            Preconditions.checkArgument(StringUtils.isNotBlank(triggerName));
            this.triggerName = triggerName;
            this.processors = new ArrayList<ChangeProcessor>();
        }

        @Override
        public String getLogIdentifier() {
            return triggerName;
        }

        @Override
        public LogProcessorBuilder setProcessorIdentifier(String name) {
            Preconditions.checkArgument(StringUtils.isNotBlank(name));
            this.readMarkerName = name;
            return this;
        }

        @Override
        public LogProcessorBuilder setStartTime(long sinceEpoch, TimeUnit unit) {
            this.startTime = new StandardTimestamp(sinceEpoch,unit);
            return this;
        }

        @Override
        public LogProcessorBuilder setStartTimeNow() {
            this.startTime = null;
            return this;
        }

        @Override
        public LogProcessorBuilder addProcessor(ChangeProcessor processor) {
            Preconditions.checkArgument(processor!=null);
            this.processors.add(processor);
            return this;
        }

        @Override
        public LogProcessorBuilder setRetryAttempts(int attempts) {
            Preconditions.checkArgument(attempts>0,"Invalid number: %s",attempts);
            this.retryAttempts = attempts;
            return this;
        }

        @Override
        public void build() {
            Preconditions.checkArgument(!processors.isEmpty(),"Must add at least one processor");
            ReadMarker readMarker;
            if (startTime==null && readMarkerName==null) {
                readMarker = ReadMarker.fromNow();
            } else if (readMarkerName==null) {
                readMarker = ReadMarker.fromTime(startTime.sinceEpoch(startTime.getNativeUnit()),startTime.getNativeUnit());
            } else if (startTime==null) {
                readMarker = ReadMarker.fromIdentifierOrNow(readMarkerName);
            } else {
                readMarker = ReadMarker.fromIdentifierOrTime(readMarkerName, startTime.sinceEpoch(startTime.getNativeUnit()), startTime.getNativeUnit());
            }
            synchronized (StandardLogProcessorFramework.this) {
                Preconditions.checkArgument(!processorLogs.containsKey(triggerName),
                        "Processors have already been registered for trigger: %s",triggerManager);
                String logName = Backend.getTriggerLogName(triggerName);
                try {
                    Log log = triggerManager.openLog(logName,readMarker);
                    log.registerReaders(Iterables.transform(processors, new Function<ChangeProcessor, MessageReader>() {
                        @Nullable
                        @Override
                        public MessageReader apply(@Nullable ChangeProcessor changeProcessor) {
                            return new MsgReaderConverter(triggerName, changeProcessor, retryAttempts);
                        }
                    }));
                } catch (StorageException e) {
                    throw new TitanException("Could not open log for trigger: "+triggerName,e);
                }
            }
        }
    }

    private class MsgReaderConverter implements MessageReader {

        private final String triggerName;
        private final ChangeProcessor processor;
        private final int retryAttempts;

        private MsgReaderConverter(String triggerName, ChangeProcessor processor, int retryAttempts) {
            this.triggerName = triggerName;
            this.processor = processor;
            this.retryAttempts = retryAttempts;
        }

        private void readRelations(ReadBuffer content, Change state,
                                   StandardTitanTx tx, StandardChangeState changes) {
            assert state.isProper();
            long numRels = VariableLong.readPositive(content);
            Preconditions.checkArgument(numRels>=0 && numRels<=Integer.MAX_VALUE);
            for (int i=0;i<numRels;i++) {
                long outVertexId = VariableLong.readPositive(content);
                Entry relEntry = BufferUtil.readEntry(content,graph.getDataSerializer());
                InternalVertex outVertex = tx.getInternalVertex(outVertexId);
                //Special relation parsing, compare to {@link RelationConstructor}
                RelationCache relCache = tx.getEdgeSerializer().readRelation(relEntry, false, tx);
                assert relCache.direction == Direction.OUT;
                InternalRelationType type = (InternalRelationType)tx.getExistingRelationType(relCache.typeId);
                assert type.getBaseType()==null;
                InternalRelation rel;
                if (type.isPropertyKey()) {
                    if (state==Change.REMOVED) {
                        rel = new StandardProperty(relCache.relationId,(PropertyKey)type,outVertex,relCache.getValue(),ElementLifeCycle.Removed);
                    } else {
                        rel = new CacheProperty(relCache.relationId,(PropertyKey)type,outVertex,relCache.getValue(),relEntry);
                    }
                } else {
                    assert type.isEdgeLabel();
                    InternalVertex otherVertex = tx.getInternalVertex(relCache.getOtherVertexId());
                    if (state==Change.REMOVED) {
                        rel = new StandardEdge(relCache.relationId, (EdgeLabel) type, outVertex, otherVertex,ElementLifeCycle.Removed);
                    } else {
                        rel = new CacheEdge(relCache.relationId, (EdgeLabel) type, outVertex, otherVertex,relEntry);
                    }
                }
                if (state==Change.REMOVED && relCache.hasProperties()) { //copy over properties
                    for (LongObjectCursor<Object> entry : relCache) {
                        rel.setPropertyDirect(tx.getExistingRelationType(entry.key),entry.value);
                    }
                }

                //Special case for vertex addition/removal
                if (rel.getType().equals(BaseKey.VertexExists) && !(outVertex instanceof TitanSchemaElement)) {
                    if (state==Change.REMOVED) { //Mark as removed
                        ((StandardVertex)outVertex).updateLifeCycle(ElementLifeCycle.Event.REMOVED);
                    }
                    changes.addVertex(outVertex,state);
                } else if (!rel.isHidden()) {
                    changes.addRelation(rel,state);
                }
            }
        }

        @Override
        public void read(Message message) {
            for (int i=1;i<=retryAttempts;i++) {
                StandardTitanTx tx = (StandardTitanTx)graph.newTransaction();
                StandardChangeState changes = new StandardChangeState();
                StandardTransactionId transactionId = null;
                try {
                    ReadBuffer content = message.getContent().asReadBuffer();
                    String senderId =  message.getSenderId();
                    long txtime = content.getLong();
                    long txid = content.getLong();
                    transactionId = new StandardTransactionId(senderId,txid,
                            new StandardTimestamp(txtime,times.getUnit()));
                    readRelations(content,Change.ADDED,tx,changes);
                    readRelations(content,Change.REMOVED,tx,changes);
                } catch (Throwable e) {
                    tx.rollback();
                    logger.error("Encountered exception [{}] when preparing processor [{}] for trigger [{}] on attempt {} of {}",
                            e.getMessage(),processor,triggerManager,i,retryAttempts);
                    logger.error("Full exception: ",e);
                    continue;
                }
                assert transactionId!=null;
                try {
                    processor.process(tx,transactionId,changes);
                    return;
                } catch (Throwable e) {
                    tx.rollback();
                    tx = null;
                    logger.error("Encountered exception [{}] when running processor [{}] for trigger [{}] on attempt {} of {}",
                            e.getMessage(),processor,triggerManager,i,retryAttempts);
                    logger.error("Full exception: ",e);
                } finally {
                    if (tx!=null) tx.commit();
                }
            }
        }
    }

}
