package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.thinkaurelius.titan.diskstorage.Mutation;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.KCVEntryMutation;

import javax.annotation.Nullable;
import java.util.List;

/**
 * {@link Mutation} for {@link KeyValueStore}.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class KVMutation extends Mutation<KeyValueEntry,StaticBuffer> {

    public KVMutation(List<KeyValueEntry> additions, List<StaticBuffer> deletions) {
        super(additions, deletions);
    }

    public KVMutation() {
        super();
    }

    private static Function<KeyValueEntry,StaticBuffer> ENTRY2KEY_FCT = new Function<KeyValueEntry, StaticBuffer>() {
        @Nullable
        @Override
        public StaticBuffer apply(@Nullable KeyValueEntry keyValueEntry) {
            return keyValueEntry.getKey();
        }
    };

    @Override
    public void consolidate() {
        super.consolidate(ENTRY2KEY_FCT, Functions.<StaticBuffer>identity());
    }

    @Override
    public boolean isConsolidated() {
        return super.isConsolidated(ENTRY2KEY_FCT, Functions.<StaticBuffer>identity());
    }

}
