// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.diskstorage.indexing;

import com.google.common.collect.ImmutableMap;
import org.janusgraph.core.Cardinality;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

//consolidate() drops a deletion which an addition of the same mutation matches, so that no addition gets swallowed by a
//deletion. The values of collection fields it drops a deletion of are additions which the document holds already
public class IndexMutationTest {

    private static final Map<String, KeyInformation> KEYS = ImmutableMap.of(
        "alias", new StandardKeyInformation(String.class, Cardinality.SET),
        "tag", new StandardKeyInformation(String.class, Cardinality.LIST),
        "name", new StandardKeyInformation(String.class, Cardinality.SINGLE));

    private static IndexMutation mutation() {
        return new IndexMutation(KEYS::get, false, false);
    }

    @Test
    public void consolidateTellsTheValuesOfCollectionFieldsWhichItDeletedAndAdds() {
        final IndexMutation mutation = mutation();
        mutation.addition(new IndexEntry("alias", "zed"));
        mutation.addition(new IndexEntry("alias", "Zed"));
        mutation.addition(new IndexEntry("tag", "t"));
        mutation.addition(new IndexEntry("name", "new"));
        mutation.deletion(new IndexEntry("alias", "zed"));
        mutation.deletion(new IndexEntry("alias", "abc"));
        mutation.deletion(new IndexEntry("tag", "t"));
        mutation.deletion(new IndexEntry("name", "old"));
        assertFalse(mutation.isReAddition(new IndexEntry("alias", "zed")));

        mutation.consolidate();

        assertEquals(Collections.singletonList("abc"),
            mutation.getDeletions().stream().map(deletion -> deletion.value).collect(Collectors.toList()));
        assertTrue(mutation.isReAddition(new IndexEntry("alias", "zed")));
        assertTrue(mutation.isReAddition(new IndexEntry("tag", "t")));
        //Another value, even of the same case-insensitive words, and a single field, whose deletion of another value
        //consolidate() drops as well, are not values the document held
        assertFalse(mutation.isReAddition(new IndexEntry("alias", "Zed")));
        assertFalse(mutation.isReAddition(new IndexEntry("name", "new")));
    }

    @Test
    public void mergeKeepsTheReAdditionsOfTheMutationMergedIn() {
        final IndexMutation consolidated = mutation();
        consolidated.addition(new IndexEntry("alias", "zed"));
        consolidated.deletion(new IndexEntry("alias", "zed"));
        consolidated.consolidate();

        final IndexMutation mutation = mutation();
        mutation.merge(consolidated);
        assertTrue(mutation.isReAddition(new IndexEntry("alias", "zed")));
    }
}
