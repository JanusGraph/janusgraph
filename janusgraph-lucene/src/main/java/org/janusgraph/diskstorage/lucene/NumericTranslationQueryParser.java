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

package org.janusgraph.diskstorage.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.graphdb.database.serialize.AttributeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NumericTranslationQueryParser extends QueryParser {
    private static final Logger log = LoggerFactory.getLogger(NumericTranslationQueryParser.class);
    private final KeyInformation.StoreRetriever storeRetriever;

    public NumericTranslationQueryParser(KeyInformation.StoreRetriever storeRetriever, String field, Analyzer analyzer) {
        super(field, analyzer);
        this.storeRetriever = storeRetriever;
    }

    @Override
    protected Query newRangeQuery(final String field, final String start, final String end, final boolean startInclusive,
                                  final boolean endInclusive) {

        Class<?> dataType = storeRetriever.get(field).getDataType();
        if (Number.class.isAssignableFrom(dataType)) {
            try {
                return getNumericRangeQuery(field, dataType, start, end, startInclusive, endInclusive);
            } catch (NumberFormatException e) {
                printNumberFormatException(field, dataType, e);
            }
        }

        return super.newRangeQuery(field, start, end, startInclusive, endInclusive);
    }

    @Override
    protected Query getRangeQuery(final String field, final String start, final String end, final boolean includeLower,
                                  final boolean includeUpper) throws ParseException {
        Class<?> dataType = storeRetriever.get(field).getDataType();
        if (Number.class.isAssignableFrom(dataType)) {
            try {
                return getNumericRangeQuery(field, dataType, start, end, includeLower, includeUpper);
            } catch (NumberFormatException e) {
                printNumberFormatException(field, dataType, e);
            }
        }

        return super.getRangeQuery(field, start, end, includeLower, includeUpper);
    }

    @Override
    protected Query newTermQuery(Term t) {
        if (t.field() == null) {
            return super.newTermQuery(t);
        }

        Class<?> dataType = storeRetriever.get(t.field()).getDataType();
        if (Number.class.isAssignableFrom(dataType)) {
            try {
                return buildNumericQuery(t.field(), t.text(), dataType);
            } catch (NumberFormatException e) {
                printNumberFormatException(t.field(), dataType, e);
            }
        }

        return super.newTermQuery(t);
    }

    @Override
    protected Query newWildcardQuery(final Term t) {
        if (t.field() == null) {
            return super.newWildcardQuery(t);
        }

        Class<?> dataType = storeRetriever.get(t.field()).getDataType();
        if (Number.class.isAssignableFrom(dataType)) {
            try {
                return buildNumericQuery(t.field(), t.text(), dataType);
            } catch (NumberFormatException e) {
                printNumberFormatException(t.field(), dataType, e);
            }
        }

        return super.newWildcardQuery(t);
    }

    private Query getNumericRangeQuery(final String field, final Class<?> type, String start, String end, final boolean includeLower,
                                       final boolean includeUpper) {
        if (AttributeUtil.isWholeNumber(type)) {
            long min = isMatchAll(start) ? Long.MIN_VALUE : Long.parseLong(start);
            long max = isMatchAll(end) ? Long.MAX_VALUE : Long.parseLong(end);
            if (!includeLower) {
                min = Math.addExact(min, 1);
            }
            if (!includeUpper) {
                max = Math.addExact(max, -1);
            }
            return LongPoint.newRangeQuery(field, min, max);
        } else {
            double min = isMatchAll(start) ? Double.NEGATIVE_INFINITY : Double.parseDouble(start);
            double max = isMatchAll(end) ? Double.POSITIVE_INFINITY : Double.parseDouble(end);
            if (!includeLower) {
                min = DoublePoint.nextUp(min);
            }
            if (!includeUpper) {
                max = DoublePoint.nextDown(max);
            }
            return DoublePoint.newRangeQuery(field, min, max);
        }
    }

    private Query buildNumericQuery(final String field, final String value, Class<?> type) {
        if (AttributeUtil.isWholeNumber(type)) {
            if (isMatchAll(value)) {
                return LongPoint.newRangeQuery(field, Long.MIN_VALUE, Long.MAX_VALUE);
            } else {
                return LongPoint.newExactQuery(field, Long.parseLong(value));
            }
        } else {
            if (isMatchAll(value)) {
                return DoublePoint.newRangeQuery(field, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
            } else {
                return DoublePoint.newExactQuery(field, Double.parseDouble(value));
            }
        }
    }

    private boolean isMatchAll(final String value) {
        return value == null || "*".equals(value);
    }

    private void printNumberFormatException(final String field, final Class<?> dataType, final NumberFormatException e) {
        log.warn("Expected Number type for " + field + ":" + dataType, e);
    }
}
