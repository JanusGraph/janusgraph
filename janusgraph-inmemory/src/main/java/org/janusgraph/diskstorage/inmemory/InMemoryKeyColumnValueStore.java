// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.diskstorage.inmemory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.MultiSlicesQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import javax.annotation.Nullable;

import static java.util.zip.Deflater.BEST_SPEED;
import static org.janusgraph.diskstorage.StaticBuffer.ARRAY_FACTORY;

/**
 * An in-memory implementation of {@link KeyColumnValueStore}.
 * This implementation is thread-safe. All data is held in memory, which means that the capacity of this store is
 * determined by the available heap space. No data is persisted and all data lost when the jvm terminates or store closed.
 *
 * The implementation also provides basic dump/restore capabilities, so that a "snapshot" of its contents can be saved to filesystem
 * and loaded back.
 *
 * Finally, it provides a means to assess the fragmentation of the data across allocated memory storage, and to defragment it if required.
 * NOTE: with usual "populate then query" and "add only" use cases, fragmentation is of no concern. Only if your application performs
 * a lot of deletes interleaved with additions for a prolong periods of time, and if you are experiencing a "bigger than expected" memory
 * footprint, is it worth looking at fragmentation report.
 *
 */

public class InMemoryKeyColumnValueStore implements KeyColumnValueStore {

    private static final boolean USE_COMPRESSION = true;
    private static final int READ_BUFFER_SIZE = 1024 * 1024 * 8;
    private static final int WRITE_BUFFER_SIZE = READ_BUFFER_SIZE;

    private static final Logger log = LoggerFactory.getLogger(InMemoryKeyColumnValueStore.class);

    private final String name;
    private final ConcurrentNavigableMap<StaticBuffer, InMemoryColumnValueStore> kcv;

    public InMemoryKeyColumnValueStore(final String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name = name;
        this.kcv = new ConcurrentSkipListMap<>();
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        InMemoryColumnValueStore cvs = kcv.get(query.getKey());
        if (cvs == null) return EntryList.EMPTY_LIST;
        else return cvs.getSlice(query, txh);
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer,EntryList> result = Maps.newHashMap();
        for (StaticBuffer key : keys) result.put(key,getSlice(new KeySliceQuery(key,query),txh));
        return result;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        InMemoryColumnValueStore cvs = kcv.get(key);
        if (cvs == null) {
            kcv.putIfAbsent(key, new InMemoryColumnValueStore());
            cvs = kcv.get(key);
        }
        cvs.mutate(additions, deletions, txh);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(final KeyRangeQuery query, final StoreTransaction txh) throws BackendException {
        return new RowIterator(kcv.subMap(query.getKeyStart(), query.getKeyEnd()).entrySet().iterator(), query, txh);
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        return new RowIterator(kcv.entrySet().iterator(), query, txh);
    }

    @Override
    public KeySlicesIterator getKeys(MultiSlicesQuery queries, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    public void clear() {
        kcv.clear();
    }

    @Override
    public void close() throws BackendException {
        kcv.clear();
    }

    public InMemoryKeyColumnValueStoreFragmentationReport createFragmentationReport(StoreTransaction txh) throws BackendException {
        int totalPageCount = 0;
        int numMultipageStores = 0;
        int[] pageLevels = new int[]{0, 1, 3, 5, 10, 100, 500};
        int[] pageCounts = new int[pageLevels.length + 1];
        int[] compressablePageLevels = new int[]{0, 1, 3, 5, 10, 100, 500};
        int[] compressablePageCounts = new int[compressablePageLevels.length + 1];
        int[] reductionLevels = new int[]{0, 1, 3, 5, 10, 100, 500};
        int[] reductionCounts = new int[reductionLevels.length + 1];
        int[] entryLevels = new int[]{3, 5, 10, 100, 500, 5000, 20000, 100000, 500000, 1000000};
        int[] entryCounts = new int[entryLevels.length + 1];
        List<InMemoryColumnValueStore> storesToDefragment = new ArrayList<>(0);

        int keysByteSize = 0;
        int numFragmentedStores = 0;
        int totalFragmentedPages = 0;
        int numCompressableStores = 0;
        int totalCompressablePages = 0;
        int totalAchievablePageReduction = 0;

        for (Map.Entry<StaticBuffer, InMemoryColumnValueStore> e : kcv.entrySet()) {
            keysByteSize += e.getKey().length();
            int i;
            int numEntries = e.getValue().numEntries(txh);
            for (i = 0; i < entryLevels.length; i++) {
                if (numEntries <= entryLevels[i]) {
                    break;
                }
            }
            entryCounts[i]++;
            if (e.getValue() instanceof InMemoryColumnValueStore) {
                InMemoryColumnValueStore pbCvs = e.getValue();
                SharedEntryBufferFragmentationReport fr = pbCvs.createFragmentationReport(txh);
                int pageCount = fr.getPageCount();
                totalPageCount += pageCount;
                if (pageCount > 1) {
                    numMultipageStores++;
                }
                for (i = 0; i < pageLevels.length; i++) {
                    if (pageCount <= pageLevels[i]) {
                        break;
                    }
                }
                pageCounts[i]++;
                if (fr.getFragmentedPageCount() > 0) {
                    numFragmentedStores++;
                    totalFragmentedPages += fr.getFragmentedPageCount();
                    if (fr.getCompressableChunksCount() > 0) {
                        numCompressableStores++;
                        totalCompressablePages += fr.getCompressablePageCount();
                        totalAchievablePageReduction += fr.getAchievablePageReduction();
                        storesToDefragment.add(pbCvs);

                        for (i = 0; i < compressablePageLevels.length; i++) {
                            if (fr.getCompressablePageCount() <= compressablePageLevels[i]) {
                                break;
                            }
                        }
                        compressablePageCounts[i]++;

                        for (i = 0; i < reductionLevels.length; i++) {
                            if (fr.getAchievablePageReduction() <= reductionLevels[i]) {
                                break;
                            }
                        }
                        reductionCounts[i]++;
                    }
                }

            }
        }

        return new InMemoryKeyColumnValueStoreFragmentationReport.Builder().name(name)
        .numStores(kcv.size())
        .numMultipageStores(numMultipageStores)
        .totalPageCount(totalPageCount)
        .numFragmentedStores(numFragmentedStores)
        .totalFragmentedPages(totalFragmentedPages)
        .numCompressableStores(numCompressableStores)
        .totalCompressablePages(totalCompressablePages)
        .totalAchievablePageReduction(totalAchievablePageReduction)
        .keysByteSize(keysByteSize)

        .entryLevels(entryLevels)
        .entryCounts(entryCounts)

        .pageLevels(pageLevels)
        .pageCounts(pageCounts)

        .compressablePageLevels(compressablePageLevels)
        .compressablePageCounts(compressablePageCounts)

        .reductionLevels(reductionLevels)
        .reductionCounts(reductionCounts)

        .storesToDefragment(storesToDefragment)
        .build();
    }

    public void quickDefragment(Collection<InMemoryColumnValueStore> stores, StoreTransaction txh) throws BackendException {
        for (InMemoryColumnValueStore cvs : stores) {
            cvs.quickDefragment(txh);
        }
    }

    public void quickDefragment(StoreTransaction txh) throws BackendException {
        quickDefragment(kcv.values(), txh);
    }

    private static OutputStream compressedOutputStream(OutputStream streamToWrap) {
        return new DeflaterOutputStream(streamToWrap, new Deflater(BEST_SPEED, true), true);
    }

    private static InputStream compressedInputStream(InputStream streamToWrap) {
        return new InflaterInputStream(streamToWrap, new Inflater(true));
    }

    public void dumpTo(Path storePath, ForkJoinPool parallelOperationsExecutor) {
        if (kcv.size() < 1)
            return;

        int numChunks = Runtime.getRuntime().availableProcessors() * 2;

        int chunkSize = kcv.size() > 1000 ? kcv.size() / numChunks : kcv.size();

        ArrayList<List<Map.Entry<StaticBuffer, InMemoryColumnValueStore>>> chunks = Lists.newArrayList(Iterators.partition(kcv.entrySet().iterator(), chunkSize));

        IntStream.range(0, chunks.size()).mapToObj(i ->
        {
            Path filePath = Paths.get(storePath.toString(), getName() + "_" + i);

            return parallelOperationsExecutor.submit(() -> dumpChunk(filePath, chunks.get(i)));
        }).collect(Collectors.toList()) //collecting here to make sure all tasks are submitted eagerly
            .stream().map(ForkJoinTask::join).collect(Collectors.toList());
    }

    private void dumpChunk(Path filePath, List<Map.Entry<StaticBuffer, InMemoryColumnValueStore>> chunk) {
        if (log.isDebugEnabled()) {
            log.debug("number of column stores in chunk " + filePath + ": " + chunk.size() + " " + Thread.currentThread().getName());
        }
        try (OutputStream rawStream = Files.newOutputStream(filePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
             BufferedOutputStream bufferedStream = new BufferedOutputStream(rawStream, WRITE_BUFFER_SIZE);
             OutputStream compressedStream = USE_COMPRESSION ? compressedOutputStream(bufferedStream) : null;
             DataOutputStream out = new DataOutputStream(USE_COMPRESSION ? compressedStream : bufferedStream)) {
            //write number of kcvs
            out.writeInt(chunk.size());

            for (Map.Entry<StaticBuffer, InMemoryColumnValueStore> e : chunk) {
                //write key length then actual key bytes
                out.writeInt(e.getKey().length());

                out.write(e.getKey().as(ARRAY_FACTORY));

                InMemoryColumnValueStore sbKcv = e.getValue();
                sbKcv.dumpTo(out);
            }

            if (log.isDebugEnabled()) {
                log.debug("finished writing chunk " + filePath + " " + Thread.currentThread().getName());
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static InMemoryKeyColumnValueStore readFrom(Path storePath, String name, ForkJoinPool parallelOperationsExecutor) throws IOException {
        InMemoryKeyColumnValueStore store = new InMemoryKeyColumnValueStore(name);

        Files.list(storePath).map(p -> parallelOperationsExecutor.submit(() -> readChunkFrom(p, store)))
            .collect(Collectors.toList()).stream() //force it to submit all tasks
            .map(ForkJoinTask::join).collect(Collectors.toList());

        return store;
    }

    private static int readChunkFrom(Path filePath, InMemoryKeyColumnValueStore store) {
        try (InputStream rawStream = Files.newInputStream(filePath, StandardOpenOption.READ);
             BufferedInputStream bufferedStream = new BufferedInputStream(rawStream, READ_BUFFER_SIZE);
             InputStream compressedStream = USE_COMPRESSION ? compressedInputStream(bufferedStream) : null;
             DataInputStream in = new DataInputStream(USE_COMPRESSION ? compressedStream : bufferedStream)) {
            int numKcvs = in.readInt();

            if (log.isDebugEnabled()) {
                log.debug("number of column stores in chunk " + filePath + ": " + numKcvs + " " + Thread.currentThread().getName());
            }

            for (int i = 0; i < numKcvs; i++) {
                int keyLength = in.readInt();
                if (log.isDebugEnabled()) {
                    log.debug("column " + i + " key size is " + keyLength);
                }
                byte[] keyData = new byte[keyLength];
                BufferPageUtils.readWholeArray(in, keyData);

                //NOTE: here we know that kcv is a concurrent map so safe to put in parallel from different chunks
                store.kcv.put(StaticArrayBuffer.of(keyData), InMemoryColumnValueStore.readFrom(in));
            }

            if (log.isDebugEnabled()) {
                log.debug("finished reading chunk " + filePath + " " + Thread.currentThread().getName());
            }

            return numKcvs;
        } catch (Exception ex) {
            throw new RuntimeException("Problem while reading chunk " + filePath + " of store " + store.getName(), ex);
        }
    }


    private static class RowIterator implements KeyIterator {
        private final Iterator<Map.Entry<StaticBuffer, InMemoryColumnValueStore>> rows;
        private final SliceQuery columnSlice;
        private final StoreTransaction transaction;

        private Map.Entry<StaticBuffer, InMemoryColumnValueStore> currentRow;
        private Map.Entry<StaticBuffer, InMemoryColumnValueStore> nextRow;
        private boolean isClosed;

        public RowIterator(Iterator<Map.Entry<StaticBuffer, InMemoryColumnValueStore>> rows,
                           @Nullable SliceQuery columns,
                           final StoreTransaction transaction) {
            this.rows = Iterators.filter(rows, entry -> entry != null && !entry.getValue().isEmpty(transaction));

            this.columnSlice = columns;
            this.transaction = transaction;
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            if (columnSlice == null)
                throw new IllegalStateException("getEntries() requires SliceQuery to be set.");

            final KeySliceQuery keySlice = new KeySliceQuery(currentRow.getKey(), columnSlice);
            return new RecordIterator<Entry>() {
                private final Iterator<Entry> items = currentRow.getValue().getSlice(keySlice, transaction).iterator();

                @Override
                public boolean hasNext() {
                    ensureOpen();
                    return items.hasNext();
                }

                @Override
                public Entry next() {
                    ensureOpen();
                    return items.next();
                }

                @Override
                public void close() {
                    isClosed = true;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Column removal not supported");
                }
            };
        }

        @Override
        public boolean hasNext() {
            ensureOpen();

            if (null != nextRow)
                return true;

            while (rows.hasNext()) {
                nextRow = rows.next();
                List<Entry> entries = nextRow.getValue().getSlice(new KeySliceQuery(nextRow.getKey(), columnSlice), transaction);
                if (null != entries && 0 < entries.size())
                    break;
            }

            return null != nextRow;
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();

            Preconditions.checkNotNull(nextRow);

            currentRow = nextRow;
            nextRow = null;

            return currentRow.getKey();
        }

        @Override
        public void close() {
            isClosed = true;
        }

        private void ensureOpen() {
            if (isClosed)
                throw new IllegalStateException("Iterator has been closed.");
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Key removal not supported");
        }
    }
}
