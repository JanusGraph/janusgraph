package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

/**
 * An index entry is a key-value pair (or field-value pair).
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexEntry {

    public final String key;
    public final Object value;

    public IndexEntry(final String key, final Object value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(StringUtils.isNotBlank(key));
        this.key=key;
        this.value=value;
    }

}
