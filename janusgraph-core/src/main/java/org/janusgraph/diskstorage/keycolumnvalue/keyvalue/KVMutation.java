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

package org.janusgraph.diskstorage.keycolumnvalue.keyvalue;

import org.janusgraph.diskstorage.Mutation;
import org.janusgraph.diskstorage.StaticBuffer;

import java.util.List;
import java.util.function.Function;

/**
 * {@link Mutation} for {@link KeyValueStore}.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class KVMutation extends Mutation<KeyValueEntry,StaticBuffer> {

    private static final Function<KeyValueEntry,StaticBuffer> ENTRY2KEY_FCT = KeyValueEntry::getKey;
    
    public KVMutation(List<KeyValueEntry> additions, List<StaticBuffer> deletions) {
        super(additions, deletions);
    }

    public KVMutation() {
        super();
    }

    @Override
    public void consolidate() {
        super.consolidate(ENTRY2KEY_FCT, Function.identity());
    }

    @Override
    public boolean isConsolidated() {
        return super.isConsolidated(ENTRY2KEY_FCT, Function.identity());
    }

}
