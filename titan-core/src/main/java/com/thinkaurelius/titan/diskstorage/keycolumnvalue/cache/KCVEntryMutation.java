package com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.Mutation;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVEntryMutation extends Mutation<Entry,Entry> {

    public KCVEntryMutation(List<Entry> additions, List<Entry> deletions) {
        super(additions, deletions);
    }

}
