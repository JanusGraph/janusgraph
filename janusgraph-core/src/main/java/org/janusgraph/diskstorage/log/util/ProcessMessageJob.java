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

import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.MessageReader;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper class for processing read messages with the registered message readers.
 * Simple implementation of a {@link Runnable}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@Slf4j
@RequiredArgsConstructor
public class ProcessMessageJob implements Runnable {

    @NonNull
    private final Message message;
    @NonNull
    private final MessageReader reader;

    @Override
    public void run() {
        try {
            log.debug("Passing {} to {}", message, reader);
            reader.read(message);
        } catch (Throwable e) {
            log.error("Encountered exception when processing message ["+message+"] by reader ["+reader+"]:",e);
        }
    }
}
