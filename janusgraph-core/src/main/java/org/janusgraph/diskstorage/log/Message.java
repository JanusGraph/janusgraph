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

package org.janusgraph.diskstorage.log;

import org.janusgraph.diskstorage.StaticBuffer;

import java.time.Instant;

/**
 * Messages which are added to and received from the {@link Log}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Message {

    /**
     * Returns the unique identifier for the sender of the message
     * @return
     */
    String getSenderId();

    /**
     * Returns the timestamp of this message in the specified time unit.
     * This is the time when the message was added to the log.
     * @return
     */
    Instant getTimestamp();

    /**
     * Returns the content of the message
     * @return
     */
    StaticBuffer getContent();

}
