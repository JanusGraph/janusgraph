package com.thinkaurelius.titan.diskstorage.solr;

import com.google.common.base.Joiner;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.servlet.SolrDispatchFilter;

import java.io.File;

public class SolrRunner {

    protected static final int NUM_SERVERS = 5;
    protected static final String[] CORES = new String[] { "store1", "store2", "vertex", "edge", "namev", "namee" };

    private static MiniSolrCloudCluster miniSolrCloudCluster;

    public static void start() throws Exception {

        String userDir = System.getProperty("user.dir");
        String solrHome = userDir.contains("titan-solr")
                ? Joiner.on(File.separator).join(userDir, "target", "test-classes", "solr")
                : Joiner.on(File.separator).join(userDir, "titan-solr", "target", "test-classes", "solr");

        File solrXml = new File(solrHome, "solr.xml");
        miniSolrCloudCluster = new MiniSolrCloudCluster(NUM_SERVERS, null, solrXml, null, null);
        for (String core : CORES) {
            uploadConfigDirToZk(core, Joiner.on(File.separator).join(solrHome, core));
        }
    }

    public static MiniSolrCloudCluster getMiniCluster() {
        return miniSolrCloudCluster;
    }

    public static void stop() throws Exception {
        System.clearProperty("solr.solrxml.location");
        System.clearProperty("zkHost");
        miniSolrCloudCluster.shutdown();
    }

    private static ZkController getZkController() {
        SolrDispatchFilter dispatchFilter =
                (SolrDispatchFilter) miniSolrCloudCluster.getJettySolrRunners().get(0).getDispatchFilter().getFilter();
        return dispatchFilter.getCores().getZkController();
    }

    protected static void uploadConfigDirToZk(String coreName, String collectionConfigDir) throws Exception {
        ZkController zkController = getZkController();
        zkController.uploadConfigDir(new File(collectionConfigDir), coreName);
    }
}
