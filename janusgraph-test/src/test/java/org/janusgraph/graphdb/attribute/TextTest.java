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

package org.janusgraph.graphdb.attribute;

import org.janusgraph.core.attribute.Cmp;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.commons.text.similarity.LevenshteinDistance;

import static org.janusgraph.core.attribute.Text.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TextTest {

    @Test
    public void testContains() {
        String text = "This world is full of 1funny surprises! A Full Yes";
        //Contains
        assertTrue(CONTAINS.test(text, "world"));
        assertTrue(CONTAINS.test(text, "wOrLD"));
        assertFalse(CONTAINS.test(text, "worl"));

        assertTrue(CONTAINS.test(text, "this"));
        assertTrue(CONTAINS.test(text, "yes"));
        assertFalse(CONTAINS.test(text, "funny"));

        assertFalse(CONTAINS.test(text, "a"));
        assertFalse(CONTAINS.test(text, "A"));

        assertTrue(CONTAINS.test(text, "surprises"));
        assertTrue(CONTAINS.test(text, "FULL"));

        assertTrue(CONTAINS.test(text, "full surprises"));
        assertTrue(CONTAINS.test(text, "full,surprises,world"));
        assertFalse(CONTAINS.test(text, "full bunny"));
        assertTrue(CONTAINS.test(text, "a world"));



        //Prefix
        assertTrue(CONTAINS_PREFIX.test(text, "worl"));
        assertTrue(CONTAINS_PREFIX.test(text, "wORl"));
        assertTrue(CONTAINS_PREFIX.test(text, "ye"));
        assertTrue(CONTAINS_PREFIX.test(text, "Y"));

        assertFalse(CONTAINS_PREFIX.test(text, "fo"));
        assertFalse(CONTAINS_PREFIX.test(text, "of 1f"));
        assertFalse(CONTAINS_PREFIX.test(text, "ses"));


        //Regex
        assertTrue(CONTAINS_REGEX.test(text, "fu[l]+"));
        assertTrue(CONTAINS_REGEX.test(text, "wor[ld]{1,2}"));
        assertTrue(CONTAINS_REGEX.test(text, "\\dfu\\w*"));

        assertFalse(CONTAINS_REGEX.test(text, "fo"));
        assertFalse(CONTAINS_REGEX.test(text, "wor[l]+"));
        assertFalse(CONTAINS_REGEX.test(text, "wor[ld]{3,5}"));


        String name = "fully funny";
        //Cmp
        assertTrue(Cmp.EQUAL.test(name.toString(), name));
        assertFalse(Cmp.NOT_EQUAL.test(name, name));
        assertFalse(Cmp.EQUAL.test("fullly funny", name));
        assertTrue(Cmp.NOT_EQUAL.test("fullly funny", name));

        //Prefix
        assertTrue(PREFIX.test(name, "fully"));
        assertTrue(PREFIX.test(name, "ful"));
        assertTrue(PREFIX.test(name, "fully fu"));
        assertFalse(PREFIX.test(name, "fun"));

        //REGEX
        assertTrue(REGEX.test(name, "(fu[ln]*y) (fu[ln]*y)"));
        assertFalse(REGEX.test(name, "(fu[l]*y) (fu[l]*y)"));
        assertTrue(REGEX.test(name, "(fu[l]*y) .*"));
        
        //FUZZY
        String shortValue = "ah";
        assertTrue(FUZZY.test(shortValue,"ah"));
        assertFalse(FUZZY.test(shortValue,"ai"));
        String mediumValue = "hop";
        assertTrue(FUZZY.test(mediumValue,"hop"));
        assertTrue(FUZZY.test(mediumValue,"hopp"));
        assertTrue(FUZZY.test(mediumValue,"hap"));
        assertFalse(FUZZY.test(mediumValue,"ha"));
        assertFalse(FUZZY.test(mediumValue,"hoopp"));
        String longValue = "surprises";
        assertTrue(FUZZY.test(longValue,"surprises"));
        assertTrue(FUZZY.test(longValue,"surpprises"));
        assertTrue(FUZZY.test(longValue,"sutprises"));
        assertTrue(FUZZY.test(longValue,"surprise"));
        assertFalse(FUZZY.test(longValue,"surppirsses"));

        //CONTAINS_FUZZY
        //Short
        assertTrue(CONTAINS_FUZZY.test(text,"is"));
        assertFalse(CONTAINS_FUZZY.test(text,"si"));
        //Medium
        assertTrue(CONTAINS_FUZZY.test(text,"full"));
        assertTrue(CONTAINS_FUZZY.test(text,"fully"));
        assertTrue(CONTAINS_FUZZY.test(text,"ful"));
        assertTrue(CONTAINS_FUZZY.test(text,"fill"));
        assertFalse(CONTAINS_FUZZY.test(text,"fu"));
        assertFalse(CONTAINS_FUZZY.test(text,"fullest"));
        //Long
        assertTrue(CONTAINS_FUZZY.test(text,"surprises"));
        assertTrue(CONTAINS_FUZZY.test(text,"Surpprises"));
        assertTrue(CONTAINS_FUZZY.test(text,"Sutrises"));
        assertTrue(CONTAINS_FUZZY.test(text,"surprise"));
        assertFalse(CONTAINS_FUZZY.test(text,"surppirsses"));
    }
}
