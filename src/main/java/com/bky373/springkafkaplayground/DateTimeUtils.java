package com.bky373.springkafkaplayground;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class DateTimeUtils {

    public static LocalDateTime toLocalDateTime(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        ZoneId zoneId = ZoneId.of("Asia/Seoul");
        return instant.atZone(zoneId)
                      .toLocalDateTime();
    }

    public static long toTimestamp(LocalDateTime localDateTime) {
        ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.of("Asis/Seoul"));
        return zonedDateTime.toInstant()
                            .toEpochMilli();
    }
}
