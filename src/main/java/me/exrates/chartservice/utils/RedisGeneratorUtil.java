package me.exrates.chartservice.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static java.util.Objects.isNull;

@Log4j2
@NoArgsConstructor(access = AccessLevel.NONE)
public final class RedisGeneratorUtil {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm_ss");

    public static String generateKey(String pairName) {
        return pairName.replace("/", "_").toLowerCase();
    }

    public static LocalDateTime generateDateTime(String key) {
        if (isNull(key)) {
            return null;
        }
        try {
            return LocalDateTime.parse(key, FORMATTER);
        } catch (Exception ex) {
            log.error("Process of parsing string to date format occurred error", ex);
            return null;
        }
    }

    public static String generateHashKey(LocalDateTime dateTime) {
        return dateTime.format(FORMATTER);
    }
}