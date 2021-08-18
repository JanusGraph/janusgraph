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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.log.Change;
import org.janusgraph.core.log.ChangeProcessor;
import org.janusgraph.core.log.LogProcessorBuilder;
import org.janusgraph.core.log.LogProcessorFramework;
import org.janusgraph.core.schema.JanusGraphSchemaElement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.MessageReader;
import org.janusgraph.diskstorage.log.ReadMarker;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.log.LogTxMeta;
import org.janusgraph.graphdb.database.log.TransactionLogHeader;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.vertices.StandardVertex;
import org.janusgraph.util.datastructures.ExceptionWrapper;
import org.janusgraph.util.system.ExecuteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardLogProcessorFramework implements LogProcessorFramework {

    private static final Logger logger =
            LoggerFactory.getLogger(StandardLogProcessorFramework.class);

    private final StandardJanusGraph graph;
    private final Serializer serializer;
    private final TimestampProvider times;
    private final Map<String,Log> processorLogs;

    private boolean isOpen = true;

    public StandardLogProcessorFramework(StandardJanusGraph graph) {
        Preconditions.checkArgument(graph!=null && graph.isOpen());
        this.graph = graph;
        this.serializer = graph.getDataSerializer();
        this.times = graph.getConfiguration().getTimestampProvider();
        this.processorLogs = new HashMap<>();
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
                throw new JanusGraphException("Could not close transaction log: "+ logIdentifier,e);
            }
            processorLogs.remove(logIdentifier);
            return true;
        } else return false;
    }

    @Override
    public synchronized void shutdown() throws JanusGraphException {
        if (!isOpen) return;
        isOpen = false;
        try {
            ExceptionWrapper exceptionWrapper = new ExceptionWrapper();
            for (Log log : processorLogs.values()) {
                ExecuteUtil.executeWithCatching(log::close, exceptionWrapper);
            }
            ExecuteUtil.throwIfException(exceptionWrapper);
            processorLogs.clear();
        } catch (BackendException e) {
            throw new JanusGraphException(e);
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
            this.processors = new ArrayList<>();
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
                    processorLogs.put(userLogName, log);
                } catch (BackendException e) {
                    throw new JanusGraphException("Could not open user transaction log for name: "+ userLogName,e);
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

        private void readRelations(TransactionLogHeader.Entry transactionEntry,
                                   StandardJanusGraphTx tx, StandardChangeState changes) {
            for (TransactionLogHeader.Modification modification : transactionEntry.getContentAsModifications(serializer)) {
                InternalRelation rel = ModificationDeserializer.parseRelation(modification,tx);

                //Special case for vertex addition/removal
                Change state = modification.state;
                if (rel.getType().equals(BaseKey.VertexExists) && !(rel.getVertex(0) instanceof JanusGraphSchemaElement)) {
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
                StandardJanusGraphTx tx = (StandardJanusGraphTx)graph.newTransaction();
                StandardChangeState changes = new StandardChangeState();
                final StandardTransactionId transactionId;
                try {
                    ReadBuffer content = message.getContent().asReadBuffer();
                    String senderId =  message.getSenderId();
                    TransactionLogHeader.Entry transactionEntry = TransactionLogHeader.parse(content, serializer, times);
                    if (transactionEntry.getMetadata().containsKey(LogTxMeta.SOURCE_TRANSACTION)) {
                        transactionId = (StandardTransactionId)transactionEntry.getMetadata().get(LogTxMeta.SOURCE_TRANSACTION);
                    } else {
                        transactionId = new StandardTransactionId(senderId,transactionEntry.getHeader().getId(), transactionEntry.getHeader().getTimestamp());
                    }
                    readRelations(transactionEntry,tx,changes);
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

        @Override
        public void updateState() {}
    }

}
