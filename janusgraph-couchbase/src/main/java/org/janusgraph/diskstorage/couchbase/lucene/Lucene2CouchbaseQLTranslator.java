/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.janusgraph.diskstorage.couchbase.lucene;

import com.couchbase.client.java.search.SearchQuery;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.janusgraph.diskstorage.couchbase.QueryFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class Lucene2CouchbaseQLTranslator {

    private static final Logger LOGGER = LoggerFactory.getLogger(Lucene2CouchbaseQLTranslator.class);
    private static final String TRANSLATOR_METHOD_NAME = "translate";
    private static final StandardQueryParser PARSER = new StandardQueryParser(new StandardAnalyzer());
    private static final Map<String, Method> TRANSLATORS = new ConcurrentHashMap<>();

    private Lucene2CouchbaseQLTranslator() {

    }

    public static SearchQuery translate(String queryString) {
        List<QueryFilter> result = new LinkedList<>();
        try {
            LOGGER.info("Translating lucene query: {}", queryString);
            Query query = PARSER.parse(queryString, "_all");
            return translate(query);
        } catch (QueryNodeException e) {
            throw new IllegalArgumentException("Failed to parse query '" + queryString + "'", e);
        }
    }

    public static SearchQuery translate(Query luceneQuery) {
        try {
            Method translator = getTranslator(luceneQuery.getClass());
            return (SearchQuery) translator.invoke(null, luceneQuery);
        } catch (Exception e) {
            throw new RuntimeException("Failed to translate " + luceneQuery.getClass().getSimpleName(), e);
        }
    }

    private static Method getTranslator(Class<? extends Query> queryType) throws NoSuchMethodException {
        String typeName = queryType.getSimpleName();
        if (!TRANSLATORS.containsKey(typeName)) {
            Method translator = Lucene2CouchbaseQLTranslator.class.getDeclaredMethod(TRANSLATOR_METHOD_NAME, queryType);
            TRANSLATORS.put(typeName, translator);
        }
        return TRANSLATORS.get(typeName);
    }

    public static SearchQuery translate(TermQuery query) {
        return SearchQuery.match(query.getTerm().text()).field(query.getTerm().field());
    }

    public static SearchQuery translate(BooleanQuery query) {
        com.couchbase.client.java.search.queries.BooleanQuery result = SearchQuery.booleans();
        List<BooleanClause> clauses = query.clauses();
        for (int i = 0; i < clauses.size(); i++) {
            BooleanClause clause = clauses.get(i);
            BooleanClause.Occur occur = clause.getOccur();
            SearchQuery clauseQuery = translate(clause.getQuery());
            if (occur == BooleanClause.Occur.FILTER || occur == BooleanClause.Occur.MUST) {
                result.must(clauseQuery);
            } else if (occur == BooleanClause.Occur.MUST_NOT) {
                result.mustNot(clauseQuery);
            } else if (occur == BooleanClause.Occur.SHOULD) {
                result.should(clauseQuery);
            }
        }

        return result;
    }

    public static SearchQuery translate(WildcardQuery query) {
        return SearchQuery.wildcard(query.getTerm().text()).field(query.getField());
    }

    public static SearchQuery translate(PhraseQuery query) {
        Term[] termArray = query.getTerms();
        int[] positions = query.getPositions();
        String phrase = IntStream.range(0, positions.length).boxed()
                .sorted(Comparator.comparingInt(i -> positions[i]))
                .map(i -> termArray[i].text())
                .collect(Collectors.joining(" "));

        return SearchQuery.matchPhrase(phrase).field(query.getField());
    }

    public static SearchQuery translate(PrefixQuery query) {
        return SearchQuery.prefix(query.getPrefix().text()).field(query.getField());
    }

    public static SearchQuery translate(MultiPhraseQuery query) {
        Term[][] terms = query.getTermArrays();
        int[] positions = query.getPositions();
        List<List<String>> phrases = new LinkedList<>();
        AtomicReference<String> field = new AtomicReference<>(null);

        IntStream.range(0, positions.length).boxed()
                .sorted(Comparator.comparingInt(i -> positions[i]))
                .map(i -> terms[i])
                .forEach(branches -> {
                    List<List<String>> newPhrases = new LinkedList<>();
                    for (List<String> phrase : phrases) {
                        for (Term branch : branches) {
                            if (field.get() == null) {
                                field.set(branch.field());
                            } else if (!field.get().equals(branch.field())) {
                                throw new IllegalArgumentException("All fields in MultiPhraseQuery must match");
                            }
                            List<String> newPhrase = new LinkedList<>(phrase);
                            newPhrase.add(branch.text());
                            newPhrases.add(newPhrase);
                        }
                    }
                    phrases.clear();
                    phrases.addAll(newPhrases);
                });

        return SearchQuery.disjuncts(phrases.stream()
                .map(phrase -> SearchQuery.phrase(phrase.stream().toArray(String[]::new)).field(field.get()))
                .toArray(SearchQuery[]::new));
    }

    public static SearchQuery translate(FuzzyQuery query) {
        return SearchQuery.match(query.getTerm().text())
                .field(query.getField())
                .fuzziness(query.getMaxEdits());
    }

    public static SearchQuery translate(RegexpQuery query) {
        return SearchQuery.regexp(query.getRegexp().text()).field(query.getField());
    }

    public static SearchQuery translate(TermRangeQuery query) {
        if (query.getLowerTerm() == null && query.getUpperTerm() == null) {
            return SearchQuery.match("*").field(query.getField());
        } else {
            com.couchbase.client.java.search.queries.TermRangeQuery result = SearchQuery.termRange()
                    .field(query.getField());
            if (query.getLowerTerm() != null) {
                result.min(query.getLowerTerm().utf8ToString(), query.includesLower());
            }
            if (query.getUpperTerm() != null) {
                result.max(query.getUpperTerm().utf8ToString(), query.includesUpper());
            }
            return result;
        }
    }

    public static SearchQuery translate(MatchAllDocsQuery query) {
        return SearchQuery.matchAll();
    }

    public static SearchQuery translate(MatchNoDocsQuery query) {
        return SearchQuery.matchNone();
    }
}
