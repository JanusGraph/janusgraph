package com.thinkaurelius.titan.diskstorage.log.kcvs;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.log.*;
import com.thinkaurelius.titan.diskstorage.util.*;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.util.system.BackgroundThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVSLog implements Log, BackendOperation.TransactionalProvider {

    private static final Logger log = LoggerFactory.getLogger(KCVSLog.class);

    public static final long TIMESLICE_MILLIS = 100 * 1000L; //100 seconds

    private final static int MIN_DELIVERY_DELAY = 50;
    private final static int BATCH_SIZE_MULTIPLIER = 5;
    private final static int CLOSE_DOWN_WAIT_MS = 5000;

    private final static int SYSTEM_PARTITION_ID = -1; //All 1s in binary representation
    private final static byte MESSAGE_COUNTER = 1;
    private final static byte MARKER_PREFIX = 2;
    private final static StaticBuffer MESSAGE_COUNTER_COLUMN = new WriteByteBuffer(1).putByte(MESSAGE_COUNTER).getStaticBuffer();

    private final KCVSLogManager manager;
    private final String name;
    private final KeyColumnValueStore store;

    private final int numBuckets;
    private final ReadMarker readMarker;
    private final int maxSendTime=0;
    private final int maxReadTime=0;
    private final int maxReadMsg = 1000;
    private final int[] readPartitionIds = {0};
    private final Serializer serializer = null;
    private final int sendBatchSize = 100;
    private final int maxSendDelay = 5000;
    private final int readLagTime = 500+maxSendDelay;

    private final AtomicLong numBucketCounter;
    private final AtomicLong numMsgCounter;

    private final List<MessageReader> readers;
    private final int numReadThreads = 1;
    private ScheduledExecutorService readExecutor;
    private MessagePuller[] msgPullers;
    private final int readPollingInterval = 10000;

    private final ArrayBlockingQueue<MessageEnvelope> outgoingMsg;
    private final SendThread sendThread;

    public KCVSLog(String name, KCVSLogManager manager, KeyColumnValueStore store, ReadMarker readMarker, Configuration config) {
        Preconditions.checkArgument(manager != null && name != null && readMarker != null && store != null);
        this.name=name;
        this.manager=manager;
        this.store=store;
        this.readMarker=readMarker;
        this.numBuckets = 1;
        Preconditions.checkArgument(numBuckets>=1 && numBuckets<=Integer.MAX_VALUE);


        this.readers = new ArrayList<MessageReader>();
        if (maxSendDelay>=MIN_DELIVERY_DELAY) {
            outgoingMsg = new ArrayBlockingQueue<MessageEnvelope>(sendBatchSize*BATCH_SIZE_MULTIPLIER);
            sendThread = new SendThread();
            sendThread.start();
        } else {
            outgoingMsg = null;
            sendThread = null;
        }

        readExecutor = null;
        msgPullers = null;

        this.numMsgCounter = new AtomicLong(readSetting(manager.senderId, MESSAGE_COUNTER_COLUMN, 0));
        this.numBucketCounter = new AtomicLong(0);
    }

    @Override
    public Future<Message> add(StaticBuffer payLoad) {
        return add(payLoad,manager.defaultPartitionId);
    }

    @Override
    public Future<Message> add(StaticBuffer payLoad, StaticBuffer key) {
        int partitionId = 0;
        //Get first 4 byte if exist in key
        for (int i=0;i<4;i++) {
            int b;
            if (key.length()>i) b = key.getByte(i) & 0xFF;
            else b = 0;
            partitionId = (partitionId<<8) + b;
        }
        assert manager.partitionBitWidth>=0 && manager.partitionBitWidth<=32;
        partitionId = partitionId>>>(32-manager.partitionBitWidth);
        return add(payLoad, partitionId);
    }

    private Future<Message> add(StaticBuffer payLoad, int partitionId) {
        Preconditions.checkArgument(partitionId>=0 && partitionId<(1<<manager.partitionBitWidth),"Invalid partition id: %s",partitionId);

        long timestamp = TimeUtility.INSTANCE.getTimeMillis();
        StaticBuffer key=getLogKey(partitionId,(int)(numBucketCounter.incrementAndGet()%numBuckets),getTimeSlice(timestamp));
        DataOutput out = serializer.getDataOutput(8+8+manager.senderId.length()+2+payLoad.length(),true);
        out.putLong(timestamp);
        out.writeObjectNotNull(manager.senderId);
        out.putLong(numMsgCounter.incrementAndGet());
        final int valuePos = out.getPosition();
        for (int i = 0; i < payLoad.length(); i++) {
            out.putByte(payLoad.getByte(i));
        }
        Entry entry=new StaticArrayEntry(out.getStaticBuffer(),valuePos);

        KCVSMessage msg = new KCVSMessage(entry.getValue(),timestamp,manager.senderId);
        FutureMessage fmsg = new FutureMessage(msg);
        MessageEnvelope envelope = new MessageEnvelope(fmsg,key,entry);
        if (outgoingMsg==null) {
            sendMessages(ImmutableList.of(envelope));
        } else {
            try {
                outgoingMsg.put(envelope);
            } catch (InterruptedException e) {
                throw new TitanException("Got interrupted waiting to send message",e);
            }
        }
        return fmsg;
    }

    private KCVSMessage parseMessage(Entry msg) {
        ReadBuffer r = msg.asReadBuffer();
        long timestamp = r.getLong();
        String senderId = serializer.readObjectNotNull(r,String.class);
        return new KCVSMessage(msg.getValue(),timestamp,senderId);
    }

    @Override
    public synchronized void registerReader(MessageReader... reader) {
        Preconditions.checkArgument(reader!=null && reader.length>0,"Must specify at least one reader");
        registerReaders(Arrays.asList(reader));
    }

    @Override
    public synchronized void registerReaders(Iterable<MessageReader> readers) {
        Preconditions.checkArgument(!Iterables.isEmpty(readers),"Must specify at least one reader");
        boolean firstRegistration = this.readers.isEmpty();
        for (MessageReader reader : readers) {
            Preconditions.checkNotNull(reader);
            if (!this.readers.contains(reader)) this.readers.add(reader);
        }
        if (firstRegistration && !this.readers.isEmpty()) {
            readExecutor = new ScheduledThreadPoolExecutor(numReadThreads,new RejectedExecutionHandler() {
                @Override
                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                    r.run();
                }
            });
            msgPullers = new MessagePuller[readPartitionIds.length*numBuckets];
            int pos = 0;
            for (int partitionId : readPartitionIds) {
                for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
                    msgPullers[pos]=new MessagePuller(partitionId,bucketId);
                    readExecutor.scheduleWithFixedDelay(msgPullers[pos],10,readPollingInterval,TimeUnit.MILLISECONDS);
                    pos++;
                }
            }
        }
    }

    @Override
    public synchronized boolean unregisterReader(MessageReader reader) {
        return this.readers.remove(reader);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() throws StorageException {
        setSetting(manager.senderId,MESSAGE_COUNTER_COLUMN,numMsgCounter.get());
        if (readExecutor!=null) readExecutor.shutdown();
        if (sendThread!=null) sendThread.close(CLOSE_DOWN_WAIT_MS);
        if (readExecutor!=null) {
            try {
                readExecutor.awaitTermination(1,TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Could not terminate reader thread pool for KCVSLog "+name+" due to interruption");
            }
            if (!readExecutor.isTerminated()) {
                readExecutor.shutdownNow();
                log.error("Reader thread pool for KCVSLog "+name+" did not shut down in time - could not clean up or set read markers");
            } else {
                for (MessagePuller puller : msgPullers) {
                    puller.close();
                }
            }
        }
        store.close();
        manager.closedLog(this);
    }

    @Override
    public StoreTransaction openTx() throws StorageException {
        return manager.storeManager.beginTransaction(new StoreTxConfig());
    }

    private void sendMessages(final List<MessageEnvelope> msgEnvelopes) {
        try {
            boolean success=BackendOperation.execute(new BackendOperation.Transactional<Boolean>() {
                @Override
                public Boolean call(StoreTransaction txh) throws StorageException {
                    ListMultimap<StaticBuffer,Entry> mutations = ArrayListMultimap.create();
                    for (MessageEnvelope env : msgEnvelopes) {
                        mutations.put(env.key,env.entry);
                    }
                    if (manager.storeManager.getFeatures().supportsBatchMutation()) {
                        Map<StaticBuffer,KCVMutation> muts = new HashMap<StaticBuffer, KCVMutation>(mutations.keySet().size());
                        for (StaticBuffer key : mutations.keySet()) {
                            muts.put(key,new KCVMutation(mutations.get(key),KeyColumnValueStore.NO_DELETIONS));
                        }
                        manager.storeManager.mutateMany(ImmutableMap.of(store.getName(),muts),txh);
                    } else {
                        for (StaticBuffer key : mutations.keySet()) {
                            store.mutate(key,mutations.get(key),KeyColumnValueStore.NO_DELETIONS,txh);
                        }
                    }
                    return Boolean.TRUE;
                }
                @Override
                public String toString() {
                    return "messageSending";
                }
            },this,maxSendTime);
            Preconditions.checkState(success);
            for (MessageEnvelope msgEnvelope : msgEnvelopes)
                msgEnvelope.message.delivered();
        } catch (TitanException e) {
            for (MessageEnvelope msgEnvelope : msgEnvelopes)
                msgEnvelope.message.failed(e);
            throw e;
        }
    }

    private class SendThread extends BackgroundThread {

        private List<MessageEnvelope> toSend;

        public SendThread() {
            super("KCVSLogSend"+name, false);
            toSend = new ArrayList<MessageEnvelope>(sendBatchSize);
        }

        private long timeSinceFirstMsg() {
            if (!toSend.isEmpty()) return Math.max(0,TimeUtility.INSTANCE.getTimeMillis()-toSend.get(0).message.getMessage().getTimestamp());
            else return 0;
        }

        private long maxWaitTime() {
            if (!toSend.isEmpty()) return Math.max(0,maxSendDelay-timeSinceFirstMsg());
            else return Long.MAX_VALUE;
        }

        @Override
        protected void waitCondition() throws InterruptedException {
            toSend.add(outgoingMsg.poll(maxWaitTime(), TimeUnit.MILLISECONDS));
        }

        @Override
        protected void action() {
            MessageEnvelope msg;
            while (toSend.size()<sendBatchSize && (msg=outgoingMsg.poll())!=null) {
                toSend.add(msg);
            }
            if (!toSend.isEmpty() && (timeSinceFirstMsg()>=maxSendDelay || toSend.size()>=sendBatchSize)) {
                sendMessages(toSend);
                toSend.clear();
            }
        }

        @Override
        protected void cleanup() {
            if (!toSend.isEmpty() || !outgoingMsg.isEmpty()) {
                //There are still messages waiting to be sent
                toSend.addAll(outgoingMsg);
                for (int i=0;i<toSend.size();i=i+sendBatchSize) {
                    List<MessageEnvelope> subset = toSend.subList(i,Math.min(toSend.size(),i+sendBatchSize));
                    sendMessages(subset);
                }
            }
        }
    }

    private static int getTimeSlice(long timestampMs) {
        long value = timestampMs/TIMESLICE_MILLIS;
        if (value>Integer.MAX_VALUE || value<0) throw new IllegalArgumentException("Timestamp overflow detected: " + timestampMs);
        return (int)value;
    }

    private StaticBuffer getLogKey(final int partitionId, final int bucketId, final int timeslice) {
        Preconditions.checkArgument(partitionId>=0 && partitionId<(1<<manager.partitionBitWidth)-1);
        Preconditions.checkArgument(bucketId>=0 && bucketId<numBuckets);
        DataOutput o = serializer.getDataOutput(3*4,false);
        o.putInt((partitionId<<(32-manager.partitionBitWidth)));
        o.putInt(bucketId);
        o.putInt(timeslice);
        return manager.hashKey(o.getStaticBuffer());
    }

    private StaticBuffer getMarkerColumn(int partitionId, int bucketId) {
        DataOutput out = serializer.getDataOutput(1+ 4 + 4,false);
        out.putByte(MARKER_PREFIX);
        out.putInt(partitionId);
        out.putInt(bucketId);
        return out.getStaticBuffer();
    }

    private long readSetting(String identifier, final StaticBuffer column, long defaultValue) {
        final StaticBuffer key = getSettingKey(identifier);
        StaticBuffer value = BackendOperation.execute(new BackendOperation.Transactional<StaticBuffer>() {
            @Override
            public StaticBuffer call(StoreTransaction txh) throws StorageException {
                return KCVSUtil.get(store,key,column,txh);
            }
            @Override
            public String toString() {
                return "messageSending";
            }
        },this,maxReadTime);
        if (value==null) return defaultValue;
        else {
            Preconditions.checkArgument(value.length()==8);
            return value.getLong(0);
        }
    }

    private StaticBuffer getSettingKey(String identifier) {
        DataOutput out = serializer.getDataOutput(4 + 2 + identifier.length(),true);
        out.putInt(SYSTEM_PARTITION_ID);
        out.writeObjectNotNull(identifier);
        return out.getStaticBuffer();
    }

    private void setSetting(String identifier, final StaticBuffer column, long value) {
        final StaticBuffer key = getSettingKey(identifier);
        final Entry add = StaticArrayEntry.of(column, ByteBufferUtil.getLongBuffer(value));
        Boolean status = BackendOperation.execute(new BackendOperation.Transactional<Boolean>() {
            @Override
            public Boolean call(StoreTransaction txh) throws StorageException {
                store.mutate(key,ImmutableList.of(add),KeyColumnValueStore.NO_DELETIONS,txh);
                return Boolean.TRUE;
            }
            @Override
            public String toString() {
                return "messageSending";
            }
        },this,maxSendTime);
    }

    private class MessagePuller implements Runnable {

        private final int bucketId;
        private final int partitionId;

        private long nextTimestamp;


        private MessagePuller(final int partitionId, final int bucketId) {
            this.bucketId = bucketId;
            this.partitionId = partitionId;
            if (!readMarker.hasIdentifier()) {
                this.nextTimestamp = readMarker.getStartTime();
            } else {
                this.nextTimestamp = readSetting(readMarker.getIdentifier(),getMarkerColumn(partitionId,bucketId),readMarker.getStartTime());
            }
        }

        @Override
        public void run() {
            final int timeslice = getTimeSlice(nextTimestamp);
            long maxTime = Math.min(TimeUtility.INSTANCE.getTimeMillis() - readLagTime, (timeslice + 1) * TIMESLICE_MILLIS);
            StaticBuffer logKey = getLogKey(partitionId,bucketId,timeslice);
            KeySliceQuery query = new KeySliceQuery(logKey, ByteBufferUtil.getLongBuffer(nextTimestamp), ByteBufferUtil.getLongBuffer(maxTime));
            query.setLimit(maxReadMsg);
            List<Entry> entries= BackendOperation.execute(getOperation(query),KCVSLog.this,maxReadTime);
            if (entries.size()>=maxReadMsg) {
                //Adjust maxTime to next timepoint
                Entry lastEntry = entries.get(entries.size()-1);
                maxTime = lastEntry.getLong(0)+1;
                //Retrieve all messages up to this adjusted timepoint (no limit this time => get all entries to that point
                query = new KeySliceQuery(logKey, ByteBufferUtil.nextBiggerBuffer(lastEntry.getColumn()),ByteBufferUtil.getLongBuffer(maxTime));
                List<Entry> extraEntries = BackendOperation.execute(getOperation(query),KCVSLog.this,maxReadTime);
                entries = new ArrayList<Entry>(entries);
                entries.addAll(extraEntries);
            }
            for (Entry entry : entries) {
                KCVSMessage message = parseMessage(entry);
                for (MessageReader reader : readers) {
                    readExecutor.submit(new ProcessMessageJob(message,reader));
                }
            }
            nextTimestamp=maxTime;
        }

        private void close() {
            if (readMarker.hasIdentifier()) {
                setSetting(readMarker.getIdentifier(),getMarkerColumn(partitionId,bucketId),nextTimestamp);
            }
        }


        private BackendOperation.Transactional<List<Entry>> getOperation(final KeySliceQuery query) {
            return new BackendOperation.Transactional<List<Entry>>() {
                @Override
                public List<Entry> call(StoreTransaction txh) throws StorageException {
                    return store.getSlice(query,txh);
                }
                @Override
                public String toString() {
                    return "messageReading@"+partitionId+":"+bucketId;
                }
            };
        }

    }

    private static class MessageEnvelope {

        final FutureMessage<KCVSMessage> message;
        final StaticBuffer key;
        final Entry entry;

        private MessageEnvelope(FutureMessage<KCVSMessage> message, StaticBuffer key, Entry entry) {
            this.message = message;
            this.key = key;
            this.entry = entry;
        }
    }

}
