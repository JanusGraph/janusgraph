package com.thinkaurelius.titan.graphdb.database.idassigner;

import com.thinkaurelius.titan.core.TitanException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IDPoolExhaustedException extends TitanException {

    public IDPoolExhaustedException(String msg) {
        super(msg);
    }

    public IDPoolExhaustedException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public IDPoolExhaustedException(Throwable cause) {
        super(cause);
    }

}
