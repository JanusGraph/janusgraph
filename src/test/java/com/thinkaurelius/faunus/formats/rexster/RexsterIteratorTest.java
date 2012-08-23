package com.thinkaurelius.faunus.formats.rexster;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.HashMap;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RexsterIteratorTest {
    @Test
    public void shouldBufferToSplitStartingInMiddle() {

        final RexsterVertexLoader loader = new MockRexsterVertexLoader(generateJsonArray(100));
        final RexsterIterator iterator = new RexsterIterator(loader, 20, 40, 16);

        assertValidIteration(iterator, 20);
    }

    @Test
    public void shouldBufferToSplitFromStart() {

        final RexsterVertexLoader loader = new MockRexsterVertexLoader(generateJsonArray(100));
        final RexsterIterator iterator = new RexsterIterator(loader, 0, 20, 16);

        assertValidIteration(iterator, 0);
    }

    @Test
    public void shouldBufferToEndStartingInMiddle() {

        final RexsterVertexLoader loader = new MockRexsterVertexLoader(generateJsonArray(100));
        final RexsterIterator iterator = new RexsterIterator(loader, 80, Long.MAX_VALUE, 16);

        assertValidIteration(iterator, 80);
    }

    @Test
    public void shouldBufferToEndFromStart() {

        final RexsterVertexLoader loader = new MockRexsterVertexLoader(generateJsonArray(20));
        final RexsterIterator iterator = new RexsterIterator(loader, 0, Long.MAX_VALUE, 16);

        assertValidIteration(iterator, 0);
    }

    @Test
    public void shouldBufferBiggerSplit() {

        final RexsterVertexLoader loader = new MockRexsterVertexLoader(generateJsonArray(40));
        final RexsterIterator iterator = new RexsterIterator(loader, 0, 20, 1000);

        assertValidIteration(iterator, 0);
    }

    private void assertValidIteration(final RexsterIterator iterator, final int dataCounterStart) {
        int counter = 0;

        int dataCounter = dataCounterStart;
        while (iterator.hasNext())  {
            final JSONObject json = iterator.next();
            Assert.assertEquals(dataCounter, json.optInt("data"));

            dataCounter++;
            counter++;
        }

        Assert.assertEquals(20, counter);
    }

    private static JSONArray generateJsonArray(final int number) {
        final JSONArray jsonArray = new JSONArray();
        for (int ix = 0; ix < number; ix++) {
            jsonArray.put(generateJsonObject(ix));
        }

        return jsonArray;
    }

    private static JSONObject generateJsonObject(final int data) {
        final Map<String, Object> m = new HashMap<String, Object>();
        m.put("data", data);
        return new JSONObject(m);
    }

    private class MockRexsterVertexLoader implements RexsterVertexLoader {
        private final JSONArray jsonArrayToPage;

        public MockRexsterVertexLoader(final JSONArray jsonArrayToPage) {
            this.jsonArrayToPage = jsonArrayToPage;
        }

        @Override
        public JSONArray getPagedVertices(long start, long end) {
            final JSONArray pagedArray = new JSONArray();

            final int lastOne = end > this.jsonArrayToPage.length() ? this.jsonArrayToPage.length() : (int) end;

            for (int ix = (int) start; ix < lastOne; ix++) {
                pagedArray.put(this.jsonArrayToPage.optJSONObject(ix));
            }

            return pagedArray;
        }
    }
}
