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

package org.janusgraph.diskstorage.keycolumnvalue.cache;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.Mutation;
import org.janusgraph.diskstorage.StaticBuffer;

import java.util.List;
import java.util.function.Function;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVEntryMutation extends Mutation<Entry,Entry> {

    public static final Function<Entry,StaticBuffer> ENTRY2COLUMN_FCT = Entry::getColumn;

    public KCVEntryMutation(List<Entry> additions, List<Entry> deletions) {
        super(additions, deletions);
    }

    @Override
    public void consolidate() {
        super.consolidate(ENTRY2COLUMN_FCT,ENTRY2COLUMN_FCT);
    }

    @Override
    public boolean isConsolidated() {
        return super.isConsolidated(ENTRY2COLUMN_FCT,ENTRY2COLUMN_FCT);
    }

}
