// Copyright 2018 JanusGraph Authors
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
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.graphdb.types.ParameterType;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * An Analyzer that allows delegating analysis to custom analyzers. The class names for the custom analyzers are
 * provided with Parameters passed to an IndexBuilder when declaring a lucene backed mixed index:
 * <pre>
 * addKey(someStringProperty, Mapping.TEXT.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(),
 *        "org.apache.lucene.analysis.fr.FrenchAnalyzer"))
 * </pre>
 * <p>
 * Custom analyzers are instantiated lazily and then cached for reuse.
 *
 * @author Olivier binda (olivier.binda@wanadoo.fr)
 */
public class LuceneCustomAnalyzer extends DelegatingAnalyzerWrapper {
    private static final String KEYWORD_ANALYZER = KeywordAnalyzer.class.getName();
    private static final String STANDARD_ANALYZER = StandardAnalyzer.class.getName();

    private final String store;
    private final KeyInformation.IndexRetriever informations;

    private final Map<String, Analyzer> analyzers = new HashMap<>();

    public LuceneCustomAnalyzer(String store, KeyInformation.IndexRetriever informations, ReuseStrategy fallbackStrategy) {
        super(fallbackStrategy);
        this.store = store;
        this.informations = informations;
        analyzers.put(KEYWORD_ANALYZER, new KeywordAnalyzer());
        analyzers.put(STANDARD_ANALYZER, new StandardAnalyzer());
    }

    @Override
    protected final Analyzer getWrappedAnalyzer(String fieldName) {
        if (LuceneIndex.DOCID.equals(fieldName)) {
            return analyzerFor(KEYWORD_ANALYZER);
        }
        final KeyInformation keyInformation = informations.get(store, LuceneIndex.getOrigFieldName(fieldName));
        if (keyInformation != null && keyInformation.getDataType().equals(UUID.class)) {
            return analyzerFor(KEYWORD_ANALYZER);
        }
        if (keyInformation == null || !String.class.isAssignableFrom(keyInformation.getDataType())) {
            return analyzerFor(STANDARD_ANALYZER);
        }
        final Parameter[] parameters = keyInformation.getParameters();
        final Mapping mapping;
        if (LuceneIndex.isDualFieldName(fieldName)) {
            mapping = LuceneIndex.getDualMapping(keyInformation);
        } else {
            // if mapping isn't present in parameters, we use Mapping.DEFAULT
            mapping = ParameterType.MAPPING.findParameter(parameters, Mapping.DEFAULT);
        }
        // at the moment, we only try to support custom analyzers for string data.
        // everything else falls through a StandardAnalyzer as was the case before
        return analyzerFor(analyzerNameFor(parameters, mapping, KEYWORD_ANALYZER, STANDARD_ANALYZER));
    }

    private static String analyzerNameFor(final Parameter[] parameters, final Mapping mapping, final String defaultStringAnalyzer,
                                          final String defaultTextAnalyzer) {
        switch (mapping) {
            case PREFIX_TREE:
                throw new RuntimeException("Prefix-tree is an unsupported mapping for string data & custom analyzers");
            case STRING:
                return ParameterType.STRING_ANALYZER.findParameter(parameters, defaultStringAnalyzer);
            case TEXT:
            case TEXTSTRING:
            case DEFAULT:// TEXT
                return ParameterType.TEXT_ANALYZER.findParameter(parameters, defaultTextAnalyzer);
            default:
                throw new RuntimeException("Not supported");
        }
    }

    private Analyzer analyzerFor(final String analyzerName) {
        if (!analyzers.containsKey(analyzerName)) {
            try {
                final Class classDefinition = Class.forName(analyzerName);
                analyzers.put(analyzerName, (Analyzer) classDefinition.newInstance());
            } catch (Exception e) {
                throw new RuntimeException("Analyzer cannot be instanciated for class " + analyzerName, e);
            }
        }
        return analyzers.get(analyzerName);
    }
}
