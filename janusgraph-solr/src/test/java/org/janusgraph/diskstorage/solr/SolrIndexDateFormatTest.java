// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.diskstorage.solr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.Date;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Dates are sent to Solr in the Gregorian calendar of the root locale. In a Thai default locale, which asks for the
 * Buddhist calendar here so that no JDK's locale data decides it, the calendar of a SimpleDateFormat counts years 543
 * ahead.
 */
@ResourceLock(Resources.LOCALE)
public class SolrIndexDateFormatTest {

    @Test
    public void formatsADateInTheGregorianCalendarWhateverTheDefaultLocale() {
        final Locale defaultLocale = Locale.getDefault();
        Locale.setDefault(Locale.forLanguageTag("th-TH-u-ca-buddhist"));
        try {
            assertEquals("1970-01-01T00:00:00.000Z", SolrIndex.toIsoDate(new Date(0)));
            assertEquals("2026-10-05T12:34:56.789Z", SolrIndex.toIsoDate(new Date(1791203696789L)));
        } finally {
            Locale.setDefault(defaultLocale);
        }
    }
}
