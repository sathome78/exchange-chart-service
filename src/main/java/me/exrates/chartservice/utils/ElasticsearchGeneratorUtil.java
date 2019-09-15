package me.exrates.chartservice.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@NoArgsConstructor(access = AccessLevel.NONE)
public final class ElasticsearchGeneratorUtil {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd_MM_yyyy");

    public static String generateIndex(LocalDate date) {
        return date.format(FORMATTER);
    }

    public static String generateId(String pairName) {
        return pairName.replace("/", "_").toLowerCase();
    }
}