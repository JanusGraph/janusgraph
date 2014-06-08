package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StorageException;
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

    private final Map<String, TitanGraph> openGraphs = new HashMap<String, TitanGraph>();

    @Override
    public Graph generateGraph(String uid) {
        String dir = BerkeleyStorageSetup.getHomeDir(uid);
        synchronized (openGraphs) {
            if (!openGraphs.containsKey(dir)) {
                log.debug("Cleaning directory {} before opening it for the first time", dir);
                try {
                    BerkeleyJEStoreManager s = new BerkeleyJEStoreManager(BerkeleyStorageSetup.getBerkeleyJEConfiguration(dir));
                    s.clearStorage();
                    s.close();
                    File dirFile = new File(dir);
                    Assert.assertFalse(dirFile.exists() && dirFile.listFiles().length > 0);
                } catch (StorageException e) {
                    throw new RuntimeException(e);
                }
            } else if (openGraphs.get(dir).isOpen()) {
                log.warn("Detected possible graph leak in Blueprints GraphTest method {} (dir={})",
                        getMostRecentMethodName(), dir);
                openGraphs.get(dir).shutdown();
            } else {
                log.debug("Opening graph on " + dir + " without cleaning");
            }
        }
        log.info("Opening graph with uid={} in directory {}", uid, dir);
        openGraphs.put(dir, TitanFactory.open(BerkeleyStorageSetup.getBerkeleyJEConfiguration(dir)));
        return openGraphs.get(dir);
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return true;
    }

    @Override
    public void cleanUp() throws StorageException {
        synchronized (openGraphs) {
            for (Map.Entry<String, TitanGraph> entry : openGraphs.entrySet()) {
                String dir = entry.getKey();
                TitanGraph g = entry.getValue();
                if (g.isOpen()) {
                    log.warn("Detected possible graph leak in Blueprints GraphTest method {} (dir={})",
                        getMostRecentMethodName(), dir);
                    g.shutdown();
                }
                BerkeleyJEStoreManager s = new BerkeleyJEStoreManager(BerkeleyStorageSetup.getBerkeleyJEConfiguration(dir));
                s.clearStorage();
                s.close();
                File dirFile = new File(dir);
                Assert.assertFalse(dirFile.exists() && dirFile.listFiles().length > 0);
            }
            openGraphs.clear();
        }
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
