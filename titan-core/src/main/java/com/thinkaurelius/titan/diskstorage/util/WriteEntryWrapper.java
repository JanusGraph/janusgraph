package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryMetaData;
import com.thinkaurelius.titan.diskstorage.WriteEntry;

import java.util.EnumMap;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class WriteEntryWrapper extends StaticArrayEntry implements WriteEntry {

    private final EnumMap<EntryMetaData,Object> metadata;

    public WriteEntryWrapper(Entry entry) {
        super(entry);
        this.metadata = new EnumMap<EntryMetaData, Object>(EntryMetaData.class);
    }

    public void setMetaData(EntryMetaData meta, Object data) {
        Preconditions.checkArgument(meta!=null && data!=null);
        metadata.put(meta,data);
    }

    public void removeMetaData(EntryMetaData meta) {
        Preconditions.checkArgument(meta!=null);
        metadata.remove(meta);
    }

    @Override
    public<O> O getMetaData(EntryMetaData meta) {
        return (O)metadata.get(meta);
    }

}
