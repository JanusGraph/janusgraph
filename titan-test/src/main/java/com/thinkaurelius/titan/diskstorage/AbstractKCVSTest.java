package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.core.time.TimestampProvider;
import com.thinkaurelius.titan.core.time.Timestamps;
import com.thinkaurelius.titan.diskstorage.util.StandardTransactionHandleConfig;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class AbstractKCVSTest {

    protected static final TimestampProvider times = Timestamps.MICRO;

    protected StandardTransactionHandleConfig getTxConfig() {
        return new StandardTransactionHandleConfig.Builder().timestamp(Timestamps.MICRO.getTime()).build();
    }

}
