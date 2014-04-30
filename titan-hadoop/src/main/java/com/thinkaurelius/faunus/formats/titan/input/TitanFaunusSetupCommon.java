package com.thinkaurelius.faunus.formats.titan.input;

import com.thinkaurelius.faunus.formats.VertexQueryFilter;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanFaunusSetupCommon implements TitanFaunusSetup {

    private static final StaticBuffer DEFAULT_COLUMN = StaticArrayBuffer.of(new byte[0]);
    private static final SliceQuery DEFAULT_SLICE_QUERY = new SliceQuery(DEFAULT_COLUMN, DEFAULT_COLUMN);

    @Override
    public SliceQuery inputSlice(final VertexQueryFilter inputFilter) {
        if (inputFilter.limit == 0) {
            throw new UnsupportedOperationException();
        } else {
            return DEFAULT_SLICE_QUERY;
        }
    }

    @Override
    public void close() {
        //Do nothing
    }

}
