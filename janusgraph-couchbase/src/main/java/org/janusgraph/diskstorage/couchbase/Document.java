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

import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.java.AsyncBucket;

/**
 * Represents a Couchbase Server {@link Document} which is stored in and retrieved from a {@link AsyncBucket}.
 *
 * @author Michael Nitschinger
 * @since 2.0
 */
public interface Document<T> {

   /**
    * The per-bucket unique ID of the {@link Document}.
    *
    * @return the document id.
    */
    String id();

   /**
    * The content of the {@link Document}.
    *
    * @return the content.
    */
    T content();

   /**
    * The last-known CAS value for the {@link Document} (0 if not set).
    *
    * @return the CAS value if set.
    */
    long cas();

   /**
    * The optional expiration time for the {@link Document} (0 if not set).
    *
    * @return the expiration time.
    */
    int expiry();

    /**
     * The optional, opaque mutation token set after a successful mutation and if enabled on
     * the environment.
     *
     * Note that the mutation token is always null, unless they are explicitly enabled on the
     * environment, the server version is supported (&gt;= 4.0.0) and the mutation operation succeeded.
     *
     * If set, it can be used for enhanced durability requirements, as well as optimized consistency
     * for N1QL queries.
     *
     * @return the mutation token if set, otherwise null.
     */
    MutationToken mutationToken();

}
