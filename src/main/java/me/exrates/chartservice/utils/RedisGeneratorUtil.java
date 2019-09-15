package me.exrates.chartservice.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Log4j2
@NoArgsConstructor(access = AccessLevel.NONE)
public final class RedisGeneratorUtil {

    private static final DateTimeFormatter FORMATTER_DATE = DateTimeFormatter.ofPattern("dd_MM_yyyy");

    public static String generateKey(LocalDate date) {
        return date.format(FORMATTER_DATE);
    }

    public static String generateHashKey(String pairName) {
        return pairName.replace("/", "_").toLowerCase();
    }
}