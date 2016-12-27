package com.thinkaurelius.titan.diskstorage.solr.transform;

import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;

public class GeoToWktConverter {
    /**
     * {@link com.thinkaurelius.titan.core.attribute.Geoshape} stores Points in the String format: point[X.0,Y.0].
     * Solr needs it to be in Well-Known Text format: POINT(X.0 Y.0)
     */
    public static String convertToWktString(Geoshape fieldValue) throws BackendException {
        if (fieldValue.getType() == Geoshape.Type.POINT) {
            Geoshape.Point point = fieldValue.getPoint();
            return "POINT(" + point.getLongitude() + " " + point.getLatitude() + ")";
        } else {
            throw new PermanentBackendException("Cannot index " + fieldValue.getType());
        }
    }
}
