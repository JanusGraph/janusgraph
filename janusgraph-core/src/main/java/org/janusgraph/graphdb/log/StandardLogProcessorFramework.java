package com.thinkaurelius.titan.graphdb.log;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanException;

import com.thinkaurelius.titan.core.log.LogProcessorBuilder;
import com.thinkaurelius.titan.core.log.LogProcessorFramework;
import com.thinkaurelius.titan.core.schema.TitanSchemaElement;
import com.thinkaurelius.titan.core.log.Change;
import com.thinkaurelius.titan.core.log.ChangeProcessor;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.log.*;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.log.LogTxMeta;
import com.thinkaurelius.titan.graphdb.database.log.TransactionLogHeader;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import com.thinkaurelius.titan.graphdb.vertices.StandardVertex;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
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
    private final Serializer serializer;
    private final TimestampProvider times;
    private final Map<String,Log> processorLogs;

    private boolean isOpen = true;

    public StandardLogProcessorFramework(StandardTitanGraph graph) {
        Preconditions.checkArgument(graph!=null && graph.isOpen());
        this.graph = graph;
        this.serializer = graph.getDataSerializer();
        this.times = graph.getConfiguration().getTimestampProvider();
        this.processorLogs = new HashMap<String, Log>();
    }

    private void checkOpen() {
        Preconditions.checkState(isOpen, "Transaction log framework has already been closed");
    }

    @Override
    public synchronized boolean removeLogProcessor(String logIdentifier) {
        checkOpen();
        if (processorLogs.containsKey(logIdentifier)) {
            try {
                processorLogs.get(logIdentifier).close();
            } catch (BackendException e) {
                throw new TitanException("Could not close transaction log: "+ logIdentifier,e);
            }
            processorLogs.remove(logIdentifier);
            return true;
        } else return false;
    }

    @Override
    public synchronized void shutdown() throws TitanException {
        if (!isOpen) return;
        isOpen = false;
        try {
            try {
                for (Log log : processorLogs.values()) {
                    log.close();
                }
                processorLogs.clear();
            } finally {
            }
        } catch (BackendException e) {
            throw new TitanException(e);
        }
    }


    @Override
    public LogProcessorBuilder addLogProcessor(String logIdentifier) {
        return new Builder(logIdentifier);
    }

    private class Builder implements LogProcessorBuilder {

        private final String userLogName;
        private final List<ChangeProcessor> processors;

        private String readMarkerName = null;
        private Instant startTime = null;
        private int retryAttempts = 1;


        private Builder(String userLogName) {
            Preconditions.checkArgument(StringUtils.isNotBlank(userLogName));
            this.userLogName = userLogName;
            this.processors = new ArrayList<ChangeProcessor>();
        }

        @Override
        public String getLogIdentifier() {
            return userLogName;
        }

        @Override
        public LogProcessorBuilder setProcessorIdentifier(String name) {
            Preconditions.checkArgument(StringUtils.isNotBlank(name));
            this.readMarkerName = name;
            return this;
        }

        @Override
        public LogProcessorBuilder setStartTime(Instant startTime) {
            this.startTime = startTime;
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
                readMarker = ReadMarker.fromTime(startTime);
            } else if (startTime==null) {
                readMarker = ReadMarker.fromIdentifierOrNow(readMarkerName);
            } else {
                readMarker = ReadMarker.fromIdentifierOrTime(readMarkerName, startTime);
            }
            synchronized (StandardLogProcessorFramework.this) {
                Preconditions.checkArgument(!processorLogs.containsKey(userLogName),
                        "Processors have already been registered for user log: %s",userLogName);
                try {
                    Log log = graph.getBackend().getUserLog(userLogName);
                    log.registerReaders(readMarker,Iterables.transform(processors, new Function<ChangeProcessor, MessageReader>() {
                        @Nullable
                        @Override
                        public MessageReader apply(@Nullable ChangeProcessor changeProcessor) {
                            return new MsgReaderConverter(userLogName, changeProcessor, retryAttempts);
                        }
                    }));
                } catch (BackendException e) {
                    throw new TitanException("Could not open user transaction log for name: "+ userLogName,e);
                }
            }
        }
    }

    private class MsgReaderConverter implements MessageReader {

        private final String userlogName;
        private final ChangeProcessor processor;
        private final int retryAttempts;

        private MsgReaderConverter(String userLogName, ChangeProcessor processor, int retryAttempts) {
            this.userlogName = userLogName;
            this.processor = processor;
            this.retryAttempts = retryAttempts;
        }

        private void readRelations(TransactionLogHeader.Entry txentry,
                                   StandardTitanTx tx, StandardChangeState changes) {
            for (TransactionLogHeader.Modification modification : txentry.getContentAsModifications(serializer)) {
                InternalRelation rel = ModificationDeserializer.parseRelation(modification,tx);

                //Special case for vertex addition/removal
                Change state = modification.state;
                if (rel.getType().equals(BaseKey.VertexExists) && !(rel.getVertex(0) instanceof TitanSchemaElement)) {
                    if (state==Change.REMOVED) { //Mark as removed
                        ((StandardVertex)rel.getVertex(0)).updateLifeCycle(ElementLifeCycle.Event.REMOVED);
                    }
                    changes.addVertex(rel.getVertex(0), state);
                } else if (!rel.isInvisible()) {
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
                    TransactionLogHeader.Entry txentry = TransactionLogHeader.parse(content, serializer, times);
                    if (txentry.getMetadata().containsKey(LogTxMeta.SOURCE_TRANSACTION)) {
                        transactionId = (StandardTransactionId)txentry.getMetadata().get(LogTxMeta.SOURCE_TRANSACTION);
                    } else {
                        transactionId = new StandardTransactionId(senderId,txentry.getHeader().getId(), txentry.getHeader().getTimestamp());
                    }
                    readRelations(txentry,tx,changes);
                } catch (Throwable e) {
                    tx.rollback();
                    logger.error("Encountered exception [{}] when preparing processor [{}] for user log [{}] on attempt {} of {}",
                            e.getMessage(),processor, userlogName,i,retryAttempts);
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
                    logger.error("Encountered exception [{}] when running processor [{}] for user log [{}] on attempt {} of {}",
                            e.getMessage(),processor, userlogName,i,retryAttempts);
                    logger.error("Full exception: ",e);
                } finally {
                    if (tx!=null) tx.commit();
                }
            }
        }
    }

}
