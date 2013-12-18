package com.thinkaurelius.titan.graphdb;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.UserModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingStore;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.junit.After;
import org.junit.Before;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanGraphBaseTest {

    public WriteConfiguration config;
    public StandardTitanGraph graph;
    public StoreFeatures features;
    public TitanTransaction tx;

    public TitanGraphBaseTest() {
    }

    public abstract WriteConfiguration getConfiguration();

    @Before
    public void setUp() throws Exception {
        this.config = getConfiguration();
        Preconditions.checkNotNull(config);
        ModifiableConfiguration configuration = new ModifiableConfiguration(GraphDatabaseConfiguration.TITAN_NS,config.clone(), BasicConfiguration.Restriction.NONE);
        configuration.set(ExpectedValueCheckingStore.LOCAL_LOCK_MEDIATOR_PREFIX, "tmp");
        Backend backend = new Backend(configuration);
        backend.initialize(configuration);
        backend.clearStorage();
        open(config);
    }

    public void open(WriteConfiguration config) {
        graph = (StandardTitanGraph) TitanFactory.open(config);
        features = graph.getConfiguration().getStoreFeatures();
        tx = graph.newTransaction();
    }

    @After
    public void tearDown() throws Exception {
        close();
    }

    public void close() {
        if (null != tx && tx.isOpen())
            tx.commit();

        if (null != graph)
            graph.shutdown();
    }

    public void newTx() {
        if (null != tx && tx.isOpen())
            tx.commit();
        //tx = graph.newThreadBoundTransaction();
        tx = graph.newTransaction();
    }

    public void clopen(Object... settings) {
        config = getConfiguration();
        if (settings!=null && settings.length>0) {
            //Parse settings
            Preconditions.checkArgument(settings.length%2==0,"Expected even number of settings: %s",settings);
            Map<TestConfigOption,Object> options = Maps.newHashMap();
            for (int i=0;i<settings.length;i=i+2) {
                Preconditions.checkArgument(settings[i] instanceof TestConfigOption,"Expected configuration option but got: %s",settings[i]);
                Preconditions.checkNotNull(settings[i+1],"Null setting at position [%s]",i+1);
                options.put((TestConfigOption)settings[i],settings[i+1]);
            }
            UserModifiableConfiguration gconf = graph.getGlobalConfiguration();
            ModifiableConfiguration lconf = new ModifiableConfiguration(GraphDatabaseConfiguration.TITAN_NS,config, BasicConfiguration.Restriction.LOCAL);
            for (Map.Entry<TestConfigOption,Object> option : options.entrySet()) {
                if (option.getKey().option.isLocal()) {
                    lconf.set(option.getKey().option,option.getValue(),option.getKey().umbrella);
                } else {
                    gconf.set(ConfigElement.getPath(option.getKey().option,option.getKey().umbrella),option.getValue(),"force");
                }
            }
            gconf.close();
            lconf.close();
        }
        close();
        Preconditions.checkNotNull(config);
        open(config);
    }


    public static final TestConfigOption option(ConfigOption option, String... umbrella) {
        return new TestConfigOption(option,umbrella);
    }

    public static final class TestConfigOption {

        public final ConfigOption option;
        public final String[] umbrella;

        public TestConfigOption(ConfigOption option, String... umbrella) {
            Preconditions.checkNotNull(option);
            this.option = option;
            if (umbrella==null) umbrella=new String[0];
            this.umbrella = umbrella;
        }
    }

}
