// Copyright 2017 JanusGraph Authors
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
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.graphdb.types.ParameterType;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Olivier binda (olivier.binda@wanadoo.fr)
 */

/**
 * JanusGraph may open different index stores and needs an analyzer for each
 * so an analyzer should be provided to build IndexWriters/IndexReaders that are used when an index is opened and documents are added
 * BUT a janusGraph may add a new indexable field, with a custom tokenizing process through Parameters, at any moment, which means that it should be possible
 * to map any field to the corresponding custom analyzer, though indexreaders are already in use.
 * <p>
 * A way to achieve that would be to use a delegating analyzer that delegates Tokenizing for a field to
 * the corresponding Analyzer (whose class you get through a Mapping)
 */
public class CustomAnalyzer extends DelegatingAnalyzerWrapper {
    private static final String KEYWORD_ANALYZER = org.apache.lucene.analysis.core.KeywordAnalyzer.class.getName();
    private static final String STANDARD_ANALYZER = org.apache.lucene.analysis.standard.StandardAnalyzer.class.getName();

    private final String store;
    private final KeyInformation.IndexRetriever informations;

    private final Map<String, Analyzer> analyzers = new HashMap<>();

    public CustomAnalyzer(String store, KeyInformation.IndexRetriever informations, ReuseStrategy fallbackStrategy) {
        super(fallbackStrategy);
        this.store = store;
        this.informations = informations;
    }

    @Override
    final protected Analyzer getWrappedAnalyzer(String fieldName) {
        Mapping mapping = null;
        String stringAnalyzerString = null;
        String textAnalyzerString = null;


        Parameter[] parameters = null;
        Class<?> clazz = null;


        KeyInformation keyInformation = informations.get(store, fieldName);
        if (keyInformation != null) {
            parameters = keyInformation.getParameters();
            clazz = keyInformation.getDataType();
        }

        if (parameters != null) {
            // if mapping isn't present in parameters, we use Mapping.DEFAULT
            mapping = ParameterType.MAPPING.findParameter(parameters, Mapping.DEFAULT);
            stringAnalyzerString = ParameterType.STRING_ANALYZER.findParameter(parameters, null);
            textAnalyzerString = ParameterType.TEXT_ANALYZER.findParameter(parameters, null);
        }
      
        String analyzerString = null;

        if (clazz != null && String.class.isAssignableFrom(clazz) && mapping != null) {
            // at the moment, we only try to support custom analyzers for string data.
            // everything else falls through a StandardAnalyzer as was the case before
            switch (mapping) {
                case TEXTSTRING:
                    throw new RuntimeException("TextString is an unsupported mapping for string data & custom analyzers");
                case PREFIX_TREE:
                    throw new RuntimeException("Prefix-tree is an unsupported mapping for string data & custom analyzers");
                case STRING:
                    analyzerString = stringAnalyzerString != null ? stringAnalyzerString : KEYWORD_ANALYZER;
                    break;
                case TEXT:
                case DEFAULT:// TEXT
                    analyzerString = textAnalyzerString;
                    break;
                default:
                    throw new RuntimeException("Not supported");
            }
        }

        // the default (& fall through) Analyzer is a StandardAnalyzer
        if ((clazz == null || !String.class.isAssignableFrom(clazz)) || mapping == null || analyzerString == null) {
            analyzerString = STANDARD_ANALYZER;
        }

        final String analyzerStringFinal = analyzerString;

        Analyzer analyzer = analyzers.get(analyzerString);
        if (analyzer == null) {
            try {
                Class classDefinition = Class.forName(analyzerStringFinal);
                analyzer = (Analyzer) classDefinition.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Analyzer cannot be instanciated for class " + analyzerStringFinal, e);
            }
            analyzers.put(analyzerString, analyzer);
        }
        return analyzer;
    }
}
