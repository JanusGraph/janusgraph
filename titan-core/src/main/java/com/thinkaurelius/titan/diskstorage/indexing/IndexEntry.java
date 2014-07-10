package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.EntryMetaData;
import com.thinkaurelius.titan.diskstorage.MetaAnnotatable;
import com.thinkaurelius.titan.diskstorage.MetaAnnotated;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * An index entry is a key-value pair (or field-value pair).
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexEntry implements MetaAnnotated, MetaAnnotatable {

    public final String field;
    public final Object value;

    public IndexEntry(final String field, final Object value) {
        this(field, value, null);
    }

    public IndexEntry(final String field, final Object value, Map<EntryMetaData, Object> metadata) {
        Preconditions.checkNotNull(field);
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(StringUtils.isNotBlank(field));

        this.field = field;
        this.value = value;

        if (metadata == null || metadata == EntryMetaData.EMPTY_METADATA)
            return;

        for (Map.Entry<EntryMetaData, Object> e : metadata.entrySet())
            setMetaData(e.getKey(), e.getValue());
    }

    //########## META DATA ############
    //copied from StaticArrayEntry

    private Map<EntryMetaData,Object> metadata = EntryMetaData.EMPTY_METADATA;

    @Override
    public synchronized Object setMetaData(EntryMetaData key, Object value) {
        if (metadata == EntryMetaData.EMPTY_METADATA)
            metadata = new EntryMetaData.Map();

        return metadata.put(key,value);
    }

    @Override
    public boolean hasMetaData() {
        return !metadata.isEmpty();
    }

    @Override
    public Map<EntryMetaData,Object> getMetaData() {
        return metadata;
    }

}
