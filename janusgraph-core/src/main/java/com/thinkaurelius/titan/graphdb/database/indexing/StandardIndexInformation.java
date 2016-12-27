package com.thinkaurelius.titan.graphdb.database.indexing;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.diskstorage.indexing.IndexFeatures;
import com.thinkaurelius.titan.diskstorage.indexing.IndexInformation;
import com.thinkaurelius.titan.diskstorage.indexing.KeyInformation;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import org.apache.commons.lang.StringUtils;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardIndexInformation implements IndexInformation {

    public static final StandardIndexInformation INSTANCE = new StandardIndexInformation();

    private static final IndexFeatures STANDARD_FEATURES = new IndexFeatures.Builder().build();

    private StandardIndexInformation() {
    }

    @Override
    public boolean supports(KeyInformation information, TitanPredicate titanPredicate) {
        return titanPredicate == Cmp.EQUAL || titanPredicate == Contain.IN;
    }

    @Override
    public boolean supports(KeyInformation information) {
        return true;
    }

    @Override
    public String mapKey2Field(String key, KeyInformation information) {
        return key;
    }

    @Override
    public IndexFeatures getFeatures() {
        return STANDARD_FEATURES;
    }
}
