// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.predicate;

import java.util.Arrays;
import java.util.List;

import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Text;
import org.junit.Assert;
import org.junit.jupiter.api.Test;


public class ConnectiveJanusGraphPTest {

    @Test
    public void testToString() {
        final AndJanusPredicate biPredicate = new AndJanusPredicate(Arrays.asList(Text.PREFIX, Cmp.EQUAL, new OrJanusPredicate(Arrays.asList(Text.PREFIX, Cmp.EQUAL))));
        final List<Object> values = Arrays.asList("john", "jo", Arrays.asList("mike","mi"));
        final ConnectiveJanusGraphP predicate = new ConnectiveJanusGraphP(biPredicate, values);
        Assert.assertEquals("and(textPrefix(john), =(jo), or(textPrefix(mike), =(mi)))", predicate.toString());
    }

    @Test
    public void testToStringOneElement() {
        final AndJanusPredicate biPredicate = new AndJanusPredicate(Arrays.asList(Text.PREFIX));
        final List<Object> values = Arrays.asList("john");
        final ConnectiveJanusGraphP predicate = new ConnectiveJanusGraphP(biPredicate, values);
        Assert.assertEquals("textPrefix(john)", predicate.toString());
    }

    @Test
    public void testToStringEmpty() {
        final AndJanusPredicate biPredicate = new AndJanusPredicate();
        final List<Object> values = Arrays.asList();
        final ConnectiveJanusGraphP predicate = new ConnectiveJanusGraphP(biPredicate, values);
        Assert.assertEquals("and()", predicate.toString());
    }
}
