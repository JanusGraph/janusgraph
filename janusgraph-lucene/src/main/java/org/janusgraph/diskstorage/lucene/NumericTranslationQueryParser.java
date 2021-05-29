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
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.tinkerpop.shaded.jackson.databind.util.StdDateFormat;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.graphdb.database.serialize.AttributeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;

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
        Class<?> dataType = getKeyDataType(field);
        if (isPossibleRangeQuery(dataType)) {
            try {
                return buildNumericRangeQuery(field, dataType, start, end, startInclusive, endInclusive);
            } catch (NumberFormatException e) {
                printNumberFormatException(field, dataType, e);
            }
        }

        return super.newRangeQuery(field, start, end, startInclusive, endInclusive);
    }

    @Override
    protected Query newFieldQuery(final Analyzer analyzer, final String field, final String queryText, final boolean quoted) throws ParseException {
        Class<?> dataType = getKeyDataType(field);
        if (isPossibleRangeQuery(dataType) || Boolean.class.equals(dataType)) {
            try {
                return buildNumericQuery(field, queryText, dataType);
            } catch (NumberFormatException e) {
                printNumberFormatException(field, dataType, e);
            }
        }
        return super.newFieldQuery(analyzer, field, queryText, quoted);
    }

    @Override
    protected Query newWildcardQuery(final Term t) {
        if (t.field() == null) {
            return super.newWildcardQuery(t);
        }
        Class<?> dataType = getKeyDataType(t.field());
        if (isPossibleRangeQuery(dataType) || Boolean.class.equals(dataType)) {
            try {
                return buildNumericQuery(t.field(), t.text(), dataType);
            } catch (NumberFormatException e) {
                printNumberFormatException(t.field(), dataType, e);
            }
        }

        return super.newWildcardQuery(t);
    }

    private Query buildNumericRangeQuery(final String field, final Class<?> type, String start, String end, final boolean includeLower,
                                         final boolean includeUpper) {
        if (AttributeUtils.isWholeNumber(type) || isTemporalType(type)) {
            long min;
            long max;
            if (isTemporalType(type)) {
                min = isMatchAll(start) ? Long.MIN_VALUE : parseDate(start).getTime();
                max = isMatchAll(end) ? Long.MAX_VALUE : parseDate(end).getTime();
            } else {
                min = isMatchAll(start) ? Long.MIN_VALUE : Long.parseLong(start);
                max = isMatchAll(end) ? Long.MAX_VALUE : Long.parseLong(end);
            }
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
        Query query;
        if (AttributeUtils.isWholeNumber(type) || isTemporalType(type)) {
            if (isMatchAll(value)) {
                query = LongPoint.newRangeQuery(field, Long.MIN_VALUE, Long.MAX_VALUE);
            } else {
                if (isTemporalType(type)) {
                    query = LongPoint.newExactQuery(field, parseDate(value).getTime());
                } else {
                    query = LongPoint.newExactQuery(field, Long.parseLong(value));
                }
            }
        } else if (Boolean.class.isAssignableFrom(type)) {
            if (isMatchAll(value)) {
                return IntPoint.newRangeQuery(field, Integer.MIN_VALUE, Integer.MAX_VALUE);
            } else {
                return IntPoint.newExactQuery(field, Boolean.parseBoolean(value) ? 1 : 0);
            }
        } else {
            if (isMatchAll(value)) {
                query = DoublePoint.newRangeQuery(field, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
            } else {
                query = DoublePoint.newExactQuery(field, Double.parseDouble(value));
            }
        }
        return query;
    }

    private Class<?> getKeyDataType(final String field) {
        KeyInformation keyInformation = storeRetriever.get(field);
        if (keyInformation == null) {
            log.warn(String.format("Could not find key information for: %s", field));
            return null;
        }
        return keyInformation.getDataType();
    }

    private Date parseDate(String value) {
        try {
            return StdDateFormat.instance.parse(value);
        } catch (java.text.ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isPossibleRangeQuery(final Class<?> dataType) {
        if (dataType == null) {
            return false;
        }
        return Number.class.isAssignableFrom(dataType) || isTemporalType(dataType);
    }

    private boolean isTemporalType(final Class<?> dataType) {
        return Date.class.equals(dataType) || Instant.class.equals(dataType);
    }

    private boolean isMatchAll(final String value) {
        return value == null || "*".equals(value);
    }

    private void printNumberFormatException(final String field, final Class<?> dataType, final NumberFormatException e) {
        log.warn("Expected Number type for " + field + ":" + dataType, e);
    }
}
