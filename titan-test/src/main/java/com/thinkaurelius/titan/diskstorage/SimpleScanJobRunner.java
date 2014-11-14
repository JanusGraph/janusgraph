package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@FunctionalInterface
public interface SimpleScanJobRunner {

    public ScanMetrics run(ScanJob job, Configuration jobConf, ConfigNamespace rootNS, String rootNSName)
            throws ExecutionException, BackendException, InterruptedException, IOException;
}
