package me.exrates.chartservice.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
@NoArgsConstructor(access = AccessLevel.NONE)
public final class PairTransformerUtil {

    public static String transform(String pairName) {
        return pairName.replace("_", "/").toUpperCase();
    }

    public static String transformBack(String pairName) {
        return pairName.replace("/", "_").toLowerCase();
    }
}