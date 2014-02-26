package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

/**
 * An index entry is a key-value pair (or field-value pair).
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexEntry {

    public final String field;
    public final Object value;

    public IndexEntry(final String field, final Object value) {
        Preconditions.checkNotNull(field);
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(StringUtils.isNotBlank(field));
        this.field = field;
        this.value=value;
    }

}
