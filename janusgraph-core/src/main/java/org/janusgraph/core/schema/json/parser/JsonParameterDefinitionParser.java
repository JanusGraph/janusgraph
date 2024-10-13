// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.core.schema.json.parser;

import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.json.creator.SchemaCreationException;
import org.janusgraph.core.schema.json.definition.JsonParameterDefinition;

import java.util.HashMap;
import java.util.Map;

public class JsonParameterDefinitionParser {

    public static final String STRING_PARAMETER_PARSER_NAME = "string";
    public static final String ENUM_PARAMETER_PARSER_NAME = "enum";
    public static final String BOOLEAN_PARAMETER_PARSER_NAME = "boolean";
    public static final String BYTE_PARAMETER_PARSER_NAME = "byte";
    public static final String SHORT_PARAMETER_PARSER_NAME = "short";
    public static final String INTEGER_PARAMETER_PARSER_NAME = "integer";
    public static final String LONG_PARAMETER_PARSER_NAME = "long";
    public static final String FLOAT_PARAMETER_PARSER_NAME = "float";
    public static final String DOUBLE_PARAMETER_PARSER_NAME = "double";

    public final Map<String, JsonParameterParser> jsonParameterParsers;

    public JsonParameterDefinitionParser() {
        jsonParameterParsers = new HashMap<>();
        jsonParameterParsers.put(STRING_PARAMETER_PARSER_NAME, new StringJsonParameterParser());
        jsonParameterParsers.put(ENUM_PARAMETER_PARSER_NAME, new EnumJsonParameterParser());
        jsonParameterParsers.put(BOOLEAN_PARAMETER_PARSER_NAME, new BooleanJsonParameterParser());
        jsonParameterParsers.put(BYTE_PARAMETER_PARSER_NAME, new ByteJsonParameterParser());
        jsonParameterParsers.put(SHORT_PARAMETER_PARSER_NAME, new ShortJsonParameterParser());
        jsonParameterParsers.put(INTEGER_PARAMETER_PARSER_NAME, new IntegerJsonParameterParser());
        jsonParameterParsers.put(LONG_PARAMETER_PARSER_NAME, new LongJsonParameterParser());
        jsonParameterParsers.put(FLOAT_PARAMETER_PARSER_NAME, new FloatJsonParameterParser());
        jsonParameterParsers.put(DOUBLE_PARAMETER_PARSER_NAME, new DoubleJsonParameterParser());
    }

    public Parameter parse(JsonParameterDefinition definition){

        String parserClassPath = definition.getParser();
        if(StringUtils.isEmpty(parserClassPath)){
            parserClassPath = STRING_PARAMETER_PARSER_NAME;
        }

        JsonParameterParser parameterParser = jsonParameterParsers.get(parserClassPath);

        if(parameterParser == null){

            try {
                Class parserClass = Class.forName(parserClassPath);
                Object instance = parserClass.newInstance();

                if (!(instance instanceof JsonParameterParser)){
                    throw new SchemaCreationException("Class "+parserClassPath+" does not implement JsonParameterParser");
                }

                parameterParser = (JsonParameterParser) instance;

            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new SchemaCreationException(e);
            }

            jsonParameterParsers.put(parserClassPath, parameterParser);
        }

        return parameterParser.parse(definition.getKey(), definition.getValue());
    }
}
