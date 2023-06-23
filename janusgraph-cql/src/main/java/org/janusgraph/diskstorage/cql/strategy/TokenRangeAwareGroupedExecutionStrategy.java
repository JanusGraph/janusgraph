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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import org.apache.commons.collections.CollectionUtils;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.cql.CQLStoreManager;
import org.janusgraph.diskstorage.cql.util.CQLSliceQueryUtil;
import org.janusgraph.diskstorage.cql.util.KeysGroup;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Strategy which groups partition keys together if they belong to the same TokenRange.
 */
public class TokenRangeAwareGroupedExecutionStrategy implements GroupedExecutionStrategy{

    private final CqlSession session;

    public TokenRangeAwareGroupedExecutionStrategy(Configuration configuration, CQLStoreManager storeManager){
        // `configuration` is ignored
        this.session = storeManager.getSession();
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

        TokenMap tokenMap = optionalTokenMap.get();
        Set<TokenRange> tokenRanges = tokenMap.getTokenRanges();

        if(CollectionUtils.isEmpty(tokenRanges)){
            withoutKeysGroupingFiller.execute(futureResult, queries, keys, txh);
            return;
        }

        Map<TokenRange, KeysGroup> keyGroupBuildersByTokenRanges = new HashMap<>(tokenRanges.size());
        final int groupLimit = Math.min(keys.size(), keysGroupingLimit);

        for(StaticBuffer key : keys){
            ByteBuffer keyByteBuffer = key.asByteBuffer();
            Token token = tokenMap.newToken(keyByteBuffer);
            TokenRange tokenRange = CQLSliceQueryUtil.findTokenRange(token, tokenRanges);
            if(tokenRange == null){
                withKeysGroupingFiller.execute(futureResult, queries, new KeysGroup(Collections.singletonList(key), Collections.singletonList(keyByteBuffer), token), txh);
            } else {
                KeysGroup keyGroup = keyGroupBuildersByTokenRanges.get(tokenRange);
                if(keyGroup == null){
                    keyGroup = new KeysGroup(groupLimit, token);
                    keyGroupBuildersByTokenRanges.put(tokenRange, keyGroup);
                }
                keyGroup.addKey(key, keyByteBuffer);
                if(keyGroup.size() >= groupLimit){
                    keyGroupBuildersByTokenRanges.put(tokenRange, new KeysGroup(groupLimit, token));
                    withKeysGroupingFiller.execute(futureResult, queries, keyGroup, txh);
                }
            }
        }

        for(KeysGroup keyGroup : keyGroupBuildersByTokenRanges.values()){
            if(!keyGroup.isEmpty()){
                withKeysGroupingFiller.execute(futureResult, queries, keyGroup, txh);
            }
        }
    }

}
