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

package org.janusgraph.olap;

import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;

public class ShortestDistanceMessageCombiner implements MessageCombiner<Long> {

    private static final ShortestDistanceMessageCombiner INSTANCE = new
            ShortestDistanceMessageCombiner();

    private ShortestDistanceMessageCombiner() {

    }

    @Override
    public Long combine(final Long messageA, final Long messageB) {
        return Math.min(messageA, messageB);
    }

    public static ShortestDistanceMessageCombiner instance() {
        return INSTANCE;
    }
}
