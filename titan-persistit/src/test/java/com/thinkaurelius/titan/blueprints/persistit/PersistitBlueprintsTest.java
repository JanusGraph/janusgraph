package com.thinkaurelius.titan.blueprints.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.blueprints.TitanBlueprintsTest;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.util.system.IOUtils;
import com.tinkerpop.blueprints.Graph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Assert;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

public class PersistitBlueprintsTest extends TitanBlueprintsTest {

    private static final String DEFAULT_DIR_NAME = "standard";
    private static Map<String, Graph> openGraphs = new HashMap<String, Graph>();

    @Override
    public Graph generateGraph() {
        return generateGraph(DEFAULT_DIR_NAME);
    }

    @Override
    public Graph generateGraph(final String subdir) {
        String dir = PersistitStorageSetup.getHomeDir() + "/" + subdir;
        Configuration config= new BaseConfiguration();
        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_DIRECTORY_KEY, dir);
        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_BACKEND_KEY, "persistit");
        Graph g = TitanFactory.open(config);
        synchronized (openGraphs) {
            openGraphs.put(dir, g);
        }
        return g;
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return false;
    }

    @Override
    public void startUp() {
        //
    }

    @Override
    public void shutDown() {
        //
        synchronized (openGraphs) {
            for (Map.Entry<String, Graph> entry : openGraphs.entrySet()) {
                String dir = entry.getKey();
                File dirFile = new File(dir);
                Assert.assertFalse(dirFile.exists() && dirFile.listFiles().length > 0);
            }
            openGraphs.clear();
        }
    }

    @Override
    public void cleanUp() throws StorageException {
        //
        synchronized (openGraphs) {
            for (Map.Entry<String, Graph> entry : openGraphs.entrySet()) {
                String dir = entry.getKey();
                File dirFile = new File(dir);
                IOUtils.deleteDirectory(dirFile, true);
                Assert.assertFalse(dirFile.exists() && dirFile.listFiles().length > 0);
            }
        }
    }

}
