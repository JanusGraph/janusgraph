package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TitanVertexProperty;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDPoolExhaustedException;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@PreInitializeConfigOptions
public class PropertyPlacementStrategy extends SimpleBulkPlacementStrategy {

    private static final Logger log =
            LoggerFactory.getLogger(PropertyPlacementStrategy.class);

    public static final ConfigOption<String> PARTITION_KEY = new ConfigOption<String>(GraphDatabaseConfiguration.IDS_NS,
            "partition-key","Partitions the graph by properties of this key", ConfigOption.Type.MASKABLE, String.class, key -> StringUtils.isNotBlank(key));


    private String key;
    private IDManager idManager;

    public PropertyPlacementStrategy(Configuration config) {
        super(config);
        setPartitionKey(config.get(PARTITION_KEY));
    }

    public PropertyPlacementStrategy(String key, int concurrentPartitions) {
        super(concurrentPartitions);
        setPartitionKey(key);
    }

    public void setPartitionKey(String key) {
        Preconditions.checkArgument(StringUtils.isNotBlank(key),"Invalid key configured: %s",key);
        this.key=key;
    }

    @Override
    public void injectIDManager(IDManager idManager) {
        Preconditions.checkNotNull(idManager);
        this.idManager=idManager;
    }


    @Override
    public int getPartition(InternalElement element) {
        if (element instanceof TitanVertex) {
            int pid = getPartitionIDbyKey((TitanVertex)element);
            if (pid>=0) return pid;
        }
        return super.getPartition(element);
    }

    @Override
    public void getPartitions(Map<InternalVertex, PartitionAssignment> vertices) {
        super.getPartitions(vertices);
        for (Map.Entry<InternalVertex, PartitionAssignment> entry : vertices.entrySet()) {
            int pid = getPartitionIDbyKey(entry.getKey());
            if (pid>=0) ((SimplePartitionAssignment)entry.getValue()).setPartitionID(pid);
        }
    }

    private int getPartitionIDbyKey(TitanVertex vertex) {
        Preconditions.checkState(idManager!=null && key!=null,"PropertyPlacementStrategy has not been initialized correctly");
        assert idManager.getPartitionBound()<=Integer.MAX_VALUE;
        int partitionBound = (int)idManager.getPartitionBound();
        TitanVertexProperty p = (TitanVertexProperty)Iterables.getFirst(vertex.query().keys(key).properties(),null);
        if (p==null) return -1;
        int hashPid = Math.abs(p.value().hashCode())%partitionBound;
        assert hashPid>=0 && hashPid<partitionBound;
        if (isExhaustedPartition(hashPid)) {
            //We keep trying consecutive partition ids until we find a non-exhausted one
            int newPid=hashPid;
            do {
                newPid = (newPid+1)%partitionBound;
                if (newPid==hashPid) //We have gone full circle - no more ids to try
                    throw new IDPoolExhaustedException("Could not find non-exhausted partition");
            } while (isExhaustedPartition(newPid));
            return newPid;
        } else return hashPid;
    }
}
