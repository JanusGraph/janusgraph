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

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.log.Message;

import org.apache.commons.lang.builder.HashCodeBuilder;

import java.time.Instant;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Abstract implementation of {@link org.janusgraph.diskstorage.log.Message} which exposes the timestamp, sender, and payload
 * of a message.
 * Particular {@link org.janusgraph.diskstorage.log.Log} implementations can extend this class.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@RequiredArgsConstructor
public abstract class AbstractMessage implements Message {

    private static final int MAX_PAYLOAD_STR_LENGTH = 400;

    @NonNull
    @Getter
    private final StaticBuffer content;
    @Getter
    private final Instant timestamp;
    @NonNull
    @Getter
    private final String senderId;

    @Override
    public String toString() {
        String paystr = content.toString();
        if (paystr.length()>MAX_PAYLOAD_STR_LENGTH) paystr=paystr.substring(0,MAX_PAYLOAD_STR_LENGTH) + "...";
        return "Message@" + timestamp + ":" + senderId + "=" + paystr;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(content).append(timestamp).append(senderId).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null || !getClass().isInstance(other)) return false;
        AbstractMessage msg = (AbstractMessage)other;
        return timestamp.equals(msg.timestamp) && senderId.equals(msg.senderId) && content.equals(msg.content);
    }
}
