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

package org.janusgraph.diskstorage.log.util;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.log.Message;

import java.time.Instant;
import java.util.Objects;

/**
 * Abstract implementation of {@link org.janusgraph.diskstorage.log.Message} which exposes the timestamp, sender, and payload
 * of a message.
 * Particular {@link org.janusgraph.diskstorage.log.Log} implementations can extend this class.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractMessage implements Message {

    private static final int MAX_PAYLOAD_STR_LENGTH = 400;

    private final StaticBuffer content;
    private final Instant timestamp;
    private final String senderId;

    protected AbstractMessage(StaticBuffer content, Instant timestamp, String senderId) {
        Preconditions.checkArgument(content !=null && senderId!=null);
        this.content = content;
        this.timestamp = timestamp;
        this.senderId = senderId;
    }

    @Override
    public String getSenderId() {
        return senderId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public StaticBuffer getContent() {
        return content;
    }

    @Override
    public String toString() {
        String payloadString = content.toString();
        if (payloadString.length()>MAX_PAYLOAD_STR_LENGTH) payloadString=payloadString.substring(0,MAX_PAYLOAD_STR_LENGTH) + "...";
        return "Message@" + timestamp + ":" + senderId + "=" + payloadString;
    }

    @Override
    public int hashCode() {
        return Objects.hash(content, timestamp, senderId);
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (!getClass().isInstance(other)) return false;
        AbstractMessage msg = (AbstractMessage)other;
        return timestamp.equals(msg.timestamp) && senderId.equals(msg.senderId) && content.equals(msg.content);
    }
}
