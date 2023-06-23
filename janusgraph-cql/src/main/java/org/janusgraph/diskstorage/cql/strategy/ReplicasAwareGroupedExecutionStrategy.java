// Copyright 2023 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.cql.strategy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.cql.CQLStoreManager;
import org.janusgraph.diskstorage.cql.util.KeysGroup;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Strategy which groups partition keys together if all their replicas (Replica set) are the same.
 */
public class ReplicasAwareGroupedExecutionStrategy implements GroupedExecutionStrategy{

    private final CqlSession session;
    private final CqlIdentifier keyspace;
    public ReplicasAwareGroupedExecutionStrategy(Configuration configuration, CQLStoreManager storeManager){
        // `configuration` is ignored
        this.session = storeManager.getSession();
        this.keyspace = CqlIdentifier.fromCql(storeManager.getKeyspaceName());
    }

    @Override
    public <R, Q> void execute(R futureResult,
                               Q queries,
                               List<StaticBuffer> keys,
                               ResultFiller<R, Q, KeysGroup> withKeysGroupingFiller,
                               ResultFiller<R, Q, List<StaticBuffer>> withoutKeysGroupingFiller,
                               StoreTransaction txh,
                               int keysGroupingLimit){

        Optional<TokenMap> optionalTokenMap = session.getMetadata().getTokenMap();
        if(!optionalTokenMap.isPresent()){
            withoutKeysGroupingFiller.execute(futureResult, queries, keys, txh);
            return;
        }

        final int groupLimit = Math.min(keys.size(), keysGroupingLimit);

        TokenMap tokenMap = optionalTokenMap.get();

        Map<Set<UUID>, KeysGroup> keyGroupBuildersByNodes = new HashMap<>();

        for(StaticBuffer key : keys){
            ByteBuffer keyByteBuffer = key.asByteBuffer();
            Token token = tokenMap.newToken(keyByteBuffer);
            Set<UUID> replicas = toReplicasUUIDs(tokenMap, token);
            if(replicas.isEmpty()){
                withKeysGroupingFiller.execute(futureResult, queries, new KeysGroup(Collections.singletonList(key), Collections.singletonList(keyByteBuffer), token), txh);
            } else {
                KeysGroup keyGroup = keyGroupBuildersByNodes.get(replicas);
                if(keyGroup == null){
                    keyGroup = new KeysGroup(groupLimit, token);
                    keyGroupBuildersByNodes.put(replicas, keyGroup);
                }
                keyGroup.addKey(key, keyByteBuffer);
                if(keyGroup.size() >= groupLimit){
                    keyGroupBuildersByNodes.put(replicas, new KeysGroup(groupLimit, token));
                    withKeysGroupingFiller.execute(futureResult, queries, keyGroup, txh);
                }
            }
        }

        for(KeysGroup keyGroup : keyGroupBuildersByNodes.values()){
            if(!keyGroup.isEmpty()){
                withKeysGroupingFiller.execute(futureResult, queries, keyGroup, txh);
            }
        }
    }

    private Set<UUID> toReplicasUUIDs(TokenMap tokenMap, Token token){
        Set<Node> replicas = tokenMap.getReplicas(keyspace, token);
        if(replicas.isEmpty()){
            return Collections.emptySet();
        }
        Set<UUID> uuids = new HashSet<>(replicas.size());
        for(Node node : replicas){
            uuids.add(node.getHostId());
        }
        return uuids;
    }
}
