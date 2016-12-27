package com.thinkaurelius.titan.util.datastructures;

import java.util.*;

/**
 * CompactMap is compact representation of the {@link Map} interface which is immutable.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CompactMap implements Map<String,Object> {

    private final String[] keys;
    private final Object[] values;

    private CompactMap(final String[] keys, final Object[] values) {
        checkKeys(keys);
        if (values==null || values.length<1) throw new IllegalArgumentException("Invalid values");
        if (values.length!=keys.length) throw new IllegalArgumentException("Keys and values do not match in length");

        this.keys= deduplicateKeys(keys);
        this.values=values;
    }

    public static final CompactMap of(final String[] keys, final Object[] values) {
        return new CompactMap(keys,values);
    }

    private static final void checkKeys(final String[] keys) {
        if (keys==null || keys.length<1) throw new IllegalArgumentException("Invalid keys");
        for (int i=0;i<keys.length;i++) if (keys[i]==null) throw new IllegalArgumentException("Key cannot be null at position " + i);

    }

    private static final Map<KeyContainer,KeyContainer> KEY_CACHE = new HashMap<KeyContainer, KeyContainer>(100);
    private static final KeyContainer KEY_HULL = new KeyContainer();

    /**
     * Deduplicates keys arrays to keep the memory footprint on CompactMap to a minimum.
     *
     * This implementation is blocking for simplicity. To improve performance in multi-threaded
     * environments, use a thread-local KEY_HULL and a concurrent hash map for KEY_CACHE.
     *
     * @param keys String array to deduplicate by checking against KEY_CACHE
     * @return A deduplicated version of the given keys array
     */
    private static final String[] deduplicateKeys(String[] keys) {
        synchronized (KEY_CACHE) {
            KEY_HULL.setKeys(keys);
            KeyContainer retrieved = KEY_CACHE.get(KEY_HULL);
            if (retrieved==null) {
                retrieved = new KeyContainer(keys);
                KEY_CACHE.put(retrieved, retrieved);
            }
            return retrieved.getKeys();
        }
    }

    @Override
    public int size() {
        return keys.length;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object o) {
        return indexOf(keys,o)>=0;
    }

    @Override
    public boolean containsValue(Object o) {
        return indexOf(values,o)>=0;
    }

    private static int indexOf(Object[] arr, Object o) {
        for (int i=0;i<arr.length;i++) if (arr[i].equals(o)) return i;
        return -1;
    }

    @Override
    public Object get(Object o) {
        int pos = indexOf(keys,o);
        if (pos>=0) return values[pos];
        else return null;
    }

    @Override
    public Object put(String s, Object o) {
        throw new UnsupportedOperationException("This map is immutable");
    }

    @Override
    public Object remove(Object o) {
        throw new UnsupportedOperationException("This map is immutable");
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> map) {
        throw new UnsupportedOperationException("This map is immutable");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("This map is immutable");
    }

    @Override
    public Set<String> keySet() {
        return new Set<String>() {
            @Override
            public int size() {
                return keys.length;
            }

            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public boolean contains(Object o) {
                return indexOf(keys,o)>=0;
            }

            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {

                    private int currentPos = -1;

                    @Override
                    public boolean hasNext() {
                        return currentPos<keys.length-1;
                    }

                    @Override
                    public String next() {
                        if (!hasNext()) throw new NoSuchElementException();
                        currentPos++;
                        return keys[currentPos];
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("This map is immutable");
                    }
                };
            }

            @Override
            public Object[] toArray() {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T[] toArray(T[] ts) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean add(String s) {
                throw new UnsupportedOperationException("This map is immutable");
            }

            @Override
            public boolean remove(Object o) {
                throw new UnsupportedOperationException("This map is immutable");
            }

            @Override
            public boolean containsAll(Collection<?> objects) {
                for (Object o : objects) if (!contains(o)) return false;
                return true;
            }

            @Override
            public boolean addAll(Collection<? extends String> strings) {
                throw new UnsupportedOperationException("This map is immutable");
            }

            @Override
            public boolean retainAll(Collection<?> objects) {
                throw new UnsupportedOperationException("This map is immutable");
            }

            @Override
            public boolean removeAll(Collection<?> objects) {
                throw new UnsupportedOperationException("This map is immutable");
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException("This map is immutable");
            }
        };
    }

    @Override
    public Collection<Object> values() {
        return Arrays.asList(values);
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return new Set<Entry<String, Object>>() {
            @Override
            public int size() {
                return keys.length;
            }

            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public boolean contains(Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator<Entry<String, Object>> iterator() {
                return new Iterator<Entry<String, Object>>() {

                    int currentPos = -1;

                    @Override
                    public boolean hasNext() {
                        return currentPos <keys.length-1;
                    }

                    @Override
                    public Entry<String, Object> next() {
                        if (!hasNext()) throw new NoSuchElementException();
                        currentPos++;
                        return new Entry<String, Object>() {

                            private final int position = currentPos;

                            @Override
                            public String getKey() {
                                return keys[position];
                            }

                            @Override
                            public Object getValue() {
                                return values[position];
                            }

                            @Override
                            public Object setValue(Object o) {
                                throw new UnsupportedOperationException("This map is immutable");
                            }
                        };
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("This map is immutable");
                    }
                };
            }

            @Override
            public Object[] toArray() {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T[] toArray(T[] ts) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean add(Entry<String, Object> stringObjectEntry) {
                throw new UnsupportedOperationException("This map is immutable");
            }

            @Override
            public boolean remove(Object o) {
                throw new UnsupportedOperationException("This map is immutable");
            }

            @Override
            public boolean containsAll(Collection<?> objects) {
                for (Object o : objects) if (!contains(o)) return false;
                return true;
            }

            @Override
            public boolean addAll(Collection<? extends Entry<String, Object>> entries) {
                throw new UnsupportedOperationException("This map is immutable");
            }

            @Override
            public boolean retainAll(Collection<?> objects) {
                throw new UnsupportedOperationException("This map is immutable");
            }

            @Override
            public boolean removeAll(Collection<?> objects) {
                throw new UnsupportedOperationException("This map is immutable");
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException("This map is immutable");
            }
        };
    }

    private static class KeyContainer {

        private String[] keys;
        private int hashcode;

        KeyContainer(final String[] keys) {
            setKeys(keys);
        }

        KeyContainer() {}

        void setKeys(final String[] keys) {
            checkKeys(keys);
            this.keys = keys;
            this.hashcode= Arrays.hashCode(keys);
        }

        public String[] getKeys() {
            return keys;
        }

        @Override
        public int hashCode() {
            return hashcode;
        }

        @Override
        public boolean equals(Object other) {
            if (this==other) return true;
            else if (!(other instanceof KeyContainer)) return false;
            return Arrays.deepEquals(keys,((KeyContainer)other).keys);
        }

        public static final KeyContainer of(String[] header) {
            return new KeyContainer(header);
        }


    }


}