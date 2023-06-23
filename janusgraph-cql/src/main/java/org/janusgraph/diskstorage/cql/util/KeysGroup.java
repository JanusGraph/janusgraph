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

package org.janusgraph.diskstorage.cql.util;

import com.datastax.oss.driver.api.core.metadata.token.Token;
import org.janusgraph.diskstorage.StaticBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KeysGroup {

    private final Token routingToken;
    private final List<StaticBuffer> keys;
    private final List<ByteBuffer> rawKeys;

    public KeysGroup(int size, Token routingToken) {
        this.keys = new ArrayList<>(size);
        this.rawKeys = new ArrayList<>(size);
        this.routingToken = routingToken;
    }

    public KeysGroup(List<StaticBuffer> keys, List<ByteBuffer> rawKeys, Token routingToken) {
        this.keys = keys;
        this.rawKeys = rawKeys;
        this.routingToken = routingToken;
    }

    public List<StaticBuffer> getKeys() {
        return keys;
    }

    public List<ByteBuffer> getRawKeys() {
        return rawKeys;
    }

    public void addKey(StaticBuffer key, ByteBuffer rawKey){
        keys.add(key);
        rawKeys.add(rawKey);
    }

    public boolean isEmpty(){
        return keys.isEmpty();
    }

    public int size(){
        return keys.size();
    }

    public Token getRoutingToken() {
        return routingToken;
    }
}
