package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJEStoreManager;
import com.tinkerpop.blueprints.Graph;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BerkeleyBlueprintsTest extends TitanBlueprintsTest {

    private static final String DEFAULT_SUBDIR = "standard";

    private static final Logger log =
            LoggerFactory.getLogger(BerkeleyBlueprintsTest.class);

    @Override
    public Graph generateGraph() {
        return generateGraph(DEFAULT_SUBDIR);
    }

    @Override
    public void beforeOpeningGraph(String uid) {
        String dir = BerkeleyStorageSetup.getHomeDir(uid);
        log.debug("Cleaning directory {} before opening it for the first time", dir);
        try {
            BerkeleyJEStoreManager s = new BerkeleyJEStoreManager(BerkeleyStorageSetup.getBerkeleyJEConfiguration(dir));
            s.clearStorage();
            s.close();
            File dirFile = new File(dir);
            Assert.assertFalse(dirFile.exists() && dirFile.listFiles().length > 0);
        } catch (BackendException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TitanGraph openGraph(String uid) {
        String dir = BerkeleyStorageSetup.getHomeDir(uid);
        return TitanFactory.open(BerkeleyStorageSetup.getBerkeleyJEConfiguration(dir));
    }

    @Override
    public void extraCleanUp(String uid) throws BackendException {
        String dir = BerkeleyStorageSetup.getHomeDir(uid);
        BerkeleyJEStoreManager s = new BerkeleyJEStoreManager(BerkeleyStorageSetup.getBerkeleyJEConfiguration(dir));
        s.clearStorage();
        s.close();
        File dirFile = new File(dir);
        Assert.assertFalse(dirFile.exists() && dirFile.listFiles().length > 0);
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return true;
    }

    @Override
    public void beforeSuite() {
        //Nothing
    }

    @Override
    public void afterSuite() {
        synchronized (openGraphs) {
            for (String dir : openGraphs.keySet()) {
                File dirFile = new File(dir);
                Assert.assertFalse(dirFile.exists() && dirFile.listFiles().length > 0);
            }
        }
    }
}
