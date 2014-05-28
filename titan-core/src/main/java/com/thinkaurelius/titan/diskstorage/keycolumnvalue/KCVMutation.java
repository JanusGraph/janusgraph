package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Functions;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.Mutation;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.KCVEntryMutation;

import java.util.List;

/**
 * {@link Mutation} type for {@link KeyColumnValueStore}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class KCVMutation extends Mutation<Entry,StaticBuffer> {

    public KCVMutation(List<Entry> additions, List<StaticBuffer> deletions) {
        super(additions, deletions);
    }

    @Override
    public void consolidate() {
        super.consolidate(KCVEntryMutation.ENTRY2COLUMN_FCT, Functions.<StaticBuffer>identity());
    }

    @Override
    public boolean isConsolidated() {
        return super.isConsolidated(KCVEntryMutation.ENTRY2COLUMN_FCT, Functions.<StaticBuffer>identity());
    }
}
