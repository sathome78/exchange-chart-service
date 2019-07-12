package me.exrates.chartservice.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@NoArgsConstructor(access = AccessLevel.NONE)
public final class RedisGeneratorUtil {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm");

    public static String generateKey(String pairName) {
        return pairName.replace("/", "_").toLowerCase();
    }

    public static LocalDateTime generateDate(String key) {
        return LocalDateTime.parse(key, FORMATTER);
    }

    public static String generateHashKey(LocalDateTime dateTime) {
        return dateTime.format(FORMATTER);
    }
}