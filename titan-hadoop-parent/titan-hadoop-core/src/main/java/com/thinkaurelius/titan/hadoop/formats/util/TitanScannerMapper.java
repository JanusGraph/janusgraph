package com.thinkaurelius.titan.hadoop.formats.util;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Run a {@link com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob}
 * via a Hadoop {@link org.apache.hadoop.mapreduce.Mapper} over the edgestore.
 */
public class TitanScannerMapper extends Mapper<StaticBuffer, Iterable<Entry>, NullWritable, NullWritable> {

    private ScanJob job;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(StaticBuffer key, Iterable<Entry> value, Context context) throws IOException, InterruptedException {
        EntryList entries;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }




    private static class SimpleEntryList implements EntryList {

        private final List<Entry> encapsulated;

        private SimpleEntryList(List<Entry> encapsulated) {
            this.encapsulated = encapsulated;
        }

        @Override
        public int size() {
            return encapsulated.size();
        }

        @Override
        public boolean isEmpty() {
            return encapsulated.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return encapsulated.contains(o);
        }

        @Override
        public Iterator<Entry> iterator() {
            return encapsulated.iterator();
        }

        @Override
        public Object[] toArray() {
            return encapsulated.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return encapsulated.toArray(a);
        }

        @Override
        public boolean add(Entry entry) {
            return encapsulated.add(entry);
        }

        @Override
        public boolean remove(Object o) {
            return encapsulated.remove(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return encapsulated.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends Entry> c) {
            return encapsulated.addAll(c);
        }

        @Override
        public boolean addAll(int index, Collection<? extends Entry> c) {
            return encapsulated.addAll(index, c);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return encapsulated.removeAll(c);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return encapsulated.retainAll(c);
        }

        @Override
        public void clear() {
            encapsulated.clear();
        }

        @Override
        public boolean equals(Object o) {
            return encapsulated.equals(o);
        }

        @Override
        public int hashCode() {
            return encapsulated.hashCode();
        }

        @Override
        public Entry get(int index) {
            return encapsulated.get(index);
        }

        @Override
        public Entry set(int index, Entry element) {
            return encapsulated.set(index, element);
        }

        @Override
        public void add(int index, Entry element) {
            encapsulated.add(index, element);
        }

        @Override
        public Entry remove(int index) {
            return encapsulated.remove(index);
        }

        @Override
        public int indexOf(Object o) {
            return encapsulated.indexOf(o);
        }

        @Override
        public int lastIndexOf(Object o) {
            return encapsulated.lastIndexOf(o);
        }

        @Override
        public ListIterator<Entry> listIterator() {
            return encapsulated.listIterator();
        }

        @Override
        public ListIterator<Entry> listIterator(int index) {
            return encapsulated.listIterator(index);
        }

        @Override
        public List<Entry> subList(int fromIndex, int toIndex) {
            return encapsulated.subList(fromIndex, toIndex);
        }

        @Override
        public Spliterator<Entry> spliterator() {
            return encapsulated.spliterator();
        }

        @Override
        public Iterator<Entry> reuseIterator() {
            return iterator();
        }

        @Override
        public int getByteSize() {
            return 1; // TODO
        }
    }
}
