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

import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.json.creator.SchemaCreationException;
import org.janusgraph.core.schema.json.definition.JsonParameterDefinition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JsonParameterDefinitionParserTest {

    private final JsonParameterDefinitionParser parser = new JsonParameterDefinitionParser();

    @Test
    void shouldParseDefinedShortcutParsers(){
        JsonParameterDefinition definition = new JsonParameterDefinition();
        definition.setKey("test");

        definition.setParser(JsonParameterDefinitionParser.STRING_PARAMETER_PARSER_NAME);
        definition.setValue("testStr");
        Assertions.assertEquals("testStr", parser.parse(definition).value());

        definition.setParser(JsonParameterDefinitionParser.ENUM_PARAMETER_PARSER_NAME);
        definition.setValue("org.janusgraph.core.schema.Mapping.TEXTSTRING");
        Assertions.assertEquals(Mapping.TEXTSTRING, parser.parse(definition).value());

        definition.setParser(JsonParameterDefinitionParser.BOOLEAN_PARAMETER_PARSER_NAME);
        definition.setValue("true");
        Assertions.assertEquals(true, parser.parse(definition).value());

        definition.setParser(JsonParameterDefinitionParser.BYTE_PARAMETER_PARSER_NAME);
        definition.setValue("127");
        Assertions.assertEquals(((byte) 127), parser.parse(definition).value());

        definition.setParser(JsonParameterDefinitionParser.SHORT_PARAMETER_PARSER_NAME);
        definition.setValue("12345");
        Assertions.assertEquals(((short) 12345), parser.parse(definition).value());

        definition.setParser(JsonParameterDefinitionParser.INTEGER_PARAMETER_PARSER_NAME);
        definition.setValue("12345678");
        Assertions.assertEquals(12345678, parser.parse(definition).value());

        definition.setParser(JsonParameterDefinitionParser.LONG_PARAMETER_PARSER_NAME);
        definition.setValue("123456789012");
        Assertions.assertEquals(123456789012L, parser.parse(definition).value());

        definition.setParser(JsonParameterDefinitionParser.FLOAT_PARAMETER_PARSER_NAME);
        definition.setValue("123.45");
        Assertions.assertEquals(123.45f, parser.parse(definition).value());

        definition.setParser(JsonParameterDefinitionParser.DOUBLE_PARAMETER_PARSER_NAME);
        definition.setValue("12345.6789012");
        Assertions.assertEquals(12345.6789012d, parser.parse(definition).value());
    }

    @Test
    void shouldParseByFullClassPathParser(){
        JsonParameterDefinition definition = new JsonParameterDefinition();
        definition.setKey("test");

        definition.setParser(EnumJsonParameterParser.class.getName());
        definition.setValue("org.janusgraph.core.schema.Mapping.TEXTSTRING");
        Assertions.assertEquals(Mapping.TEXTSTRING, parser.parse(definition).value());
    }

    @Test
    void shouldFailParsingOnWrongClass(){
        JsonParameterDefinition definition = new JsonParameterDefinition();
        definition.setKey("test");
        definition.setValue("org.janusgraph.core.schema.Mapping.TEXTSTRING");

        definition.setParser(Integer.class.getName());
        Assertions.assertThrows(SchemaCreationException.class, () -> parser.parse(definition));

        definition.setParser("Unknown class");
        Assertions.assertThrows(SchemaCreationException.class, () -> parser.parse(definition));

        definition.setParser("integer");
        Assertions.assertThrows(NumberFormatException.class, () -> parser.parse(definition));
    }
}
