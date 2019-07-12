package me.exrates.chartservice.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@NoArgsConstructor(access = AccessLevel.NONE)
public final class ElasticsearchGeneratorUtil {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm");

    public static String generateIndex(String pairName) {
        return pairName.replace("/", "_").toLowerCase();
    }

    public static String generateId(LocalDateTime dateTime) {
        return dateTime.format(FORMATTER);
    }
}