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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Common parent implementation of a {@link Document}.
 *
 * It is recommended that all {@link Document} implementations extend from this class so that parameter checks
 * are consistently applied. It also ensures that equals and hashcode are applied on the contents and therefore
 * comparisons work as expected.
 *
 * @author Michael Nitschinger
 * @since 2.0.0
 */
public abstract class AbstractDocument<T> implements Document<T> {

    public static final int MAX_ID_LENGTH = 240;
    private String id;
    private long cas;
    private int expiry;
    private T content;
    private MutationToken mutationToken;

    /**
     * Constructor needed for possible subclass serialization.
     */
    protected AbstractDocument() {
    }

    protected AbstractDocument(String id, int expiry, T content, long cas) {
        this(id, expiry, content, cas, null);
    }

    protected AbstractDocument(String id, int expiry, T content, long cas, MutationToken mutationToken) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("The Document ID must not be null or empty.");
        }
        // Quick sanity check, but not 100% accurate. UTF-8 encoding avoided because of double
        // allocations, it is done in core with proper exact error handling anyways.
        if (id.length() > MAX_ID_LENGTH) {
            throw new IllegalArgumentException("The Document ID must not be larger than 250 bytes");
        }
        if (expiry < 0) {
            throw new IllegalArgumentException("The Document expiry must not be negative.");
        }

        this.id = id;
        this.cas = cas;
        this.expiry = expiry;
        this.content = content;
        this.mutationToken = mutationToken;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public long cas() {
        return cas;
    }

    @Override
    public int expiry() {
        return expiry;
    }

    @Override
    public T content() {
        return content;
    }

    @Override
    public MutationToken mutationToken() {
        return mutationToken;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(this.getClass().getSimpleName() + "{");
        sb.append("id='").append(id).append('\'');
        sb.append(", cas=").append(cas);
        sb.append(", expiry=").append(expiry);
        sb.append(", content=").append(content);
        sb.append(", mutationToken=").append(mutationToken);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractDocument<?> that = (AbstractDocument<?>) o;

        if (cas != that.cas) return false;
        if (expiry != that.expiry) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (content != null ? !content.equals(that.content) : that.content != null) return false;
        return !(mutationToken != null ? !mutationToken.equals(that.mutationToken) : that.mutationToken != null);

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (int) (cas ^ (cas >>> 32));
        result = 31 * result + expiry;
        result = 31 * result + (content != null ? content.hashCode() : 0);
        result = 31 * result + (mutationToken != null ? mutationToken.hashCode() : 0);
        return result;
    }

    /**
     * Helper method to write the current document state to the output stream for serialization purposes.
     *
     * @param stream the stream to write to.
     * @throws IOException when a stream problem occurs
     */
    protected void writeToSerializedStream(ObjectOutputStream stream) throws IOException {
        stream.writeLong(cas);
        stream.writeInt(expiry);
        stream.writeUTF(id);
        stream.writeObject(content);
        stream.writeObject(mutationToken);
    }

    /**
     * Helper method to create the document from an object input stream, used for serialization purposes.
     *
     * @param stream the stream to read from.
     * @throws IOException when a stream problem occurs
     * @throws ClassNotFoundException when requested class is not present
     */
    @SuppressWarnings("unchecked")
    protected void readFromSerializedStream(final ObjectInputStream stream) throws IOException, ClassNotFoundException {
        cas = stream.readLong();
        expiry = stream.readInt();
        id = stream.readUTF();
        content = (T) stream.readObject();
        mutationToken = (MutationToken) stream.readObject();
    }
}
