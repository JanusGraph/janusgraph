/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.janusgraph.diskstorage.couchbase;

public class CouchbaseColumn implements Comparable<CouchbaseColumn> {
    // attributes keys of json document
    public static final String ID = "id";
    public static final String TABLE = "table";
    public static final String COLUMNS = "columns";
    public static final String KEY = "key";
    public static final String VALUE = "value";
    public static final String EXPIRE = "expire";
    public static final String TTL = "ttl";
    // instance members
    private String key;
    private String value;
    private long expire;
    private int ttl;

    public CouchbaseColumn(String key, String value, long expire, int ttl) {
        this.key = key;
        this.value = value;
        this.expire = expire;
        this.ttl = ttl;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getExpire() {
        return expire;
    }

    public int getTtl() {
        return ttl;
    }

    public int compareTo(CouchbaseColumn o) {
        return key.compareTo(o.key);
    }

    public boolean equals(Object anObject) {
        if (this == anObject) {
            return true;
        }
        if (anObject instanceof CouchbaseColumn) {
            CouchbaseColumn anotherColumn = (CouchbaseColumn)anObject;
            return key.equals(anotherColumn.key);
        }
        return false;
    }

    public int hashCode() {
        return key.hashCode();
    }
}
