package com.thinkaurelius.titan.graphdb.database.indexing;

import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.diskstorage.indexing.IndexInformation;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardIndexInformation implements IndexInformation {

    public static final StandardIndexInformation INSTANCE = new StandardIndexInformation();

    private StandardIndexInformation() {}

    @Override
    public boolean supports(Class<?> dataType, Relation relation) {
        return relation==Cmp.EQUAL;
    }

    @Override
    public boolean supports(Class<?> dataType) {
        return true;
    }
}
