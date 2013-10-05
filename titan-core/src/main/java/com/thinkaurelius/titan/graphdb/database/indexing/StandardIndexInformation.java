package com.thinkaurelius.titan.graphdb.database.indexing;

import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.diskstorage.indexing.IndexInformation;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardIndexInformation implements IndexInformation {

    public static final StandardIndexInformation INSTANCE = new StandardIndexInformation();

    private StandardIndexInformation() {
    }

    @Override
    public boolean supports(Class<?> dataType, TitanPredicate titanPredicate) {
        return titanPredicate == Cmp.EQUAL || titanPredicate == Contain.IN;
    }

    @Override
    public boolean supports(Class<?> dataType) {
        return true;
    }

}
