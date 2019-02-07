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
