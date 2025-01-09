package com.bky373.springkafkaplayground.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class DateTimeUtils {

    private static final ZoneId DEFAULT_ZONE_ID = ZoneId.of("Asia/Seoul");

    public static LocalDateTime toLocalDateTime(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        ZoneId zoneId = ZoneId.of("Asia/Seoul");
        return instant.atZone(zoneId)
                      .toLocalDateTime();
    }

    public static long toTimestamp(LocalDateTime localDateTime) {
        return localDateTime.atZone(DEFAULT_ZONE_ID)
                            .toInstant()
                            .toEpochMilli();
    }
}
