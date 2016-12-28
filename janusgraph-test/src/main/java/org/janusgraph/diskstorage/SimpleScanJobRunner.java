package org.janusgraph.diskstorage;

import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@FunctionalInterface
public interface SimpleScanJobRunner {

    public ScanMetrics run(ScanJob job, Configuration jobConf, String rootNSName)
            throws ExecutionException, BackendException, InterruptedException, IOException;
}
