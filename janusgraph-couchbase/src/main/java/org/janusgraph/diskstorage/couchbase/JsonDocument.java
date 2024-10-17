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
import com.couchbase.client.java.json.JsonObject;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Represents a {@link Document} that contains a {@link JsonObject} as the content.
 *
 * The {@link JsonDocument} is one of the most integral parts of the API. It is intended to be used as a canonical
 * wrapper around retrieval and mutation operations, abstracting away JSON internals.
 *
 * Note that there is no public constructor available, but rather a multitude of factory methods that allow you to work
 * nicely with this immutable value object. It is possible to construct empty/fresh ones, but also copies will be
 * created from passed in documents, allowing you to override specific parts during the copy process.
 *
 * This document is interoperable with other SDKs.
 *
 * It can always be the case that some or all fields of a {@link JsonDocument} are not set, depending on the operation
 * performed. Here are the accessible fields and their default values:
 *
 * +---------------+---------+
 * | Field         | Default |
 * +---------------+---------+
 * | id            | null    |
 * | content       | null    |
 * | cas           | 0       |
 * | expiry        | 0       |
 * | status        | null    |
 * | mutationToken | null    |
 * +---------------+---------+
 *
 * @author Michael Nitschinger
 * @since 2.0
 */
public class JsonDocument extends AbstractDocument<JsonObject> implements Serializable {

    private static final long serialVersionUID = 2050104986260610101L;

    /**
     * Creates a {@link JsonDocument} which the document id.
     *
     * @param id the per-bucket unique document id.
     * @return a {@link JsonDocument}.
     */
    public static JsonDocument create(String id) {
        return new JsonDocument(id, 0, null, 0, null);
    }

    /**
     * Creates a {@link JsonDocument} which the document id and JSON content.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @return a {@link JsonDocument}.
     */
    public static JsonDocument create(String id, JsonObject content) {
        return new JsonDocument(id, 0, content, 0, null);
    }

    /**
     * Creates a {@link JsonDocument} which the document id, JSON content and the CAS value.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @return a {@link JsonDocument}.
     */
    public static JsonDocument create(String id, JsonObject content, long cas) {
        return new JsonDocument(id, 0, content, cas, null);
    }

    /**
     * Creates a {@link JsonDocument} which the document id, JSON content and the expiration time.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param expiry the expiration time of the document.
     * @return a {@link JsonDocument}.
     */
    public static JsonDocument create(String id, int expiry, JsonObject content) {
        return new JsonDocument(id, expiry, content, 0, null);
    }

    /**
     * Creates a {@link JsonDocument} which the document id, JSON content, CAS value, expiration time and status code.
     *
     * This factory method is normally only called within the client library when a response is analyzed and a document
     * is returned which is enriched with the status code. It does not make sense to pre populate the status field from
     * the user level code.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @param expiry the expiration time of the document.
     * @return a {@link JsonDocument}.
     */
    public static JsonDocument create(String id, int expiry, JsonObject content, long cas) {
        return new JsonDocument(id, expiry, content, cas, null);
    }

    /**
     * Creates a {@link JsonDocument} which the document id, JSON content, CAS value, expiration time and status code.
     *
     * This factory method is normally only called within the client library when a response is analyzed and a document
     * is returned which is enriched with the status code. It does not make sense to pre populate the status field from
     * the user level code.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @param expiry the expiration time of the document.
     * @param mutationToken mutation token
     * @return a {@link JsonDocument}.
     */
    public static JsonDocument create(String id, int expiry, JsonObject content, long cas, MutationToken mutationToken) {
        return new JsonDocument(id, expiry, content, cas, mutationToken);
    }

    /**
     * Creates a copy from a different {@link JsonDocument}, but changes the document ID.
     *
     * @param doc the original {@link JsonDocument} to copy.
     * @param id the per-bucket unique document id.
     * @return a copied {@link JsonDocument} with the changed properties.
     */
    public static JsonDocument from(JsonDocument doc, String id) {
        return JsonDocument.create(id, doc.expiry(), doc.content(), doc.cas(), doc.mutationToken());
    }

    /**
     * Creates a copy from a different {@link JsonDocument}, but changes the content.
     *
     * @param doc the original {@link JsonDocument} to copy.
     * @param content the content of the document.
     * @return a copied {@link JsonDocument} with the changed properties.
     */
    public static JsonDocument from(JsonDocument doc, JsonObject content) {
        return JsonDocument.create(doc.id(), doc.expiry(), content, doc.cas(), doc.mutationToken());
    }

    /**
     * Creates a copy from a different {@link JsonDocument}, but changes the document ID and content.
     *
     * @param doc the original {@link JsonDocument} to copy.
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @return a copied {@link JsonDocument} with the changed properties.
     */
    public static JsonDocument from(JsonDocument doc, String id, JsonObject content) {
        return JsonDocument.create(id, doc.expiry(), content, doc.cas(), doc.mutationToken());
    }

    /**
     * Creates a copy from a different {@link JsonDocument}, but changes the CAS value.
     *
     * @param doc the original {@link JsonDocument} to copy.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @return a copied {@link JsonDocument} with the changed properties.
     */
    public static JsonDocument from(JsonDocument doc, long cas) {
        return JsonDocument.create(doc.id(), doc.expiry(), doc.content(), cas, doc.mutationToken());
    }

    /**
     * Private constructor which is called by the static factory methods eventually.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @param expiry the expiration time of the document.
     */
    private JsonDocument(String id, int expiry, JsonObject content, long cas, MutationToken mutationToken) {
        super(id, expiry, content, cas, mutationToken);
    }

    private void writeObject(ObjectOutputStream stream) throws IOException {
        writeToSerializedStream(stream);
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        readFromSerializedStream(stream);
    }

}
