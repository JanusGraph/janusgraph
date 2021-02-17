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

package org.janusgraph.graphdb.database.serialize.attribute;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Longs;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.serialize.OrderPreservingSerializer;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Date;
import java.util.Locale;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

public class DateSerializer implements OrderPreservingSerializer<Date> {

    private static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);

    // Equivalent to ISO_LOCAL_DATE_TIME with optional time
    private static final DateTimeFormatter LENIENT_ISO_LOCAL_DATE_TIME;
    static {
        LENIENT_ISO_LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .optionalStart()
            .appendLiteral('T')
            .append(ISO_LOCAL_TIME)
            .optionalEnd()
            .toFormatter(Locale.ROOT);
    }

    private static final DateTimeFormatter TIME_OFFSET_WITHOUT_COLON = new DateTimeFormatterBuilder()
        .appendOffset("+HHmm", "Z")
        .toFormatter(Locale.ROOT);

    // Equivalent to ISO_DATE_TIME with optional time and offsets without colons
    private static final DateTimeFormatter LENIENT_ISO_DATE_TIME;
    static {
        LENIENT_ISO_DATE_TIME = new DateTimeFormatterBuilder()
            .append(LENIENT_ISO_LOCAL_DATE_TIME)
            .optionalStart()
            .appendZoneOrOffsetId()
            .optionalEnd()
            .optionalStart()
            .append(TIME_OFFSET_WITHOUT_COLON)
            .optionalEnd()
            .toFormatter(Locale.ROOT);
    }

    private final LongSerializer ls = LongSerializer.INSTANCE;

    @Override
    public Date read(ScanBuffer buffer) {
        long utc = ls.read(buffer);
        return new Date(utc);
    }

    @Override
    public void write(WriteBuffer out, Date attribute) {
        long utc = attribute.getTime();
        ls.write(out, utc);
    }

    @Override
    public Date readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Date attribute) {
        write(buffer,attribute);
    }

    @Override
    public Date convert(Object value) {
        if (value instanceof Number && !(value instanceof Float) && !(value instanceof Double)) {
            return new Date(((Number)value).longValue());
        } else if (value instanceof String) {
            String s = (String) value;

            Long v = Longs.tryParse(s);
            if (v != null) {
                return new Date(v);
            }

            if (s.contains(" ")) {
                return dateFromTemporalAccessor(RFC_1123_DATE_TIME.parse(s));
            }

            return dateFromTemporalAccessor(LENIENT_ISO_DATE_TIME.parse(s));
        }
        return null;
    }

    /**
     * Ensures no exception is thrown and sensitive defaults are used for date, time and zone.
     */
    private static Date dateFromTemporalAccessor(TemporalAccessor accessor) {
        LocalDate localDate = MoreObjects.firstNonNull(accessor.query(TemporalQueries.localDate()), EPOCH);
        LocalTime localTime = MoreObjects.firstNonNull(accessor.query(TemporalQueries.localTime()), LocalTime.MIDNIGHT);
        ZoneId zone = MoreObjects.firstNonNull(accessor.query(TemporalQueries.zone()), ZoneOffset.UTC);
        return Date.from(ZonedDateTime.of(localDate, localTime, zone).toInstant());
    }
}
