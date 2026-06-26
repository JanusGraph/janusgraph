// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.diskstorage.inmemory;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/** Test backend: delegates to in-memory but records every mutateMany for assertions. */
public class WholeRowDeletionCapturingStoreManager extends InMemoryStoreManager {

    public static final class Captured {
        public final String store;
        public final StaticBuffer key;
        public final boolean wholeRow;
        public final int deletions;
        public final int additions;

        Captured(String store, StaticBuffer key, boolean wholeRow, int deletions, int additions) {
            this.store = store;
            this.key = key;
            this.wholeRow = wholeRow;
            this.deletions = deletions;
            this.additions = additions;
        }
    }

    public static final List<Captured> CAPTURED = new CopyOnWriteArrayList<>();

    public static void reset() {
        CAPTURED.clear();
    }

    public WholeRowDeletionCapturingStoreManager(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        mutations.forEach((store, km) -> km.forEach((key, m) ->
            CAPTURED.add(new Captured(store, key, m.hasWholeRowDeletion(), m.getDeletions().size(), m.getAdditions().size()))));
        super.mutateMany(mutations, txh);
    }
}
