package me.exrates.chartservice.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.exceptions.CurrencyPairFormatException;

import java.util.function.Predicate;
import java.util.regex.Pattern;

@Log4j2
@NoArgsConstructor(access = AccessLevel.NONE)
public final class OpenApiUtil {

    private static final Predicate<String> CURRENCY_PAIR_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9.]{1,8}[_-][a-zA-Z0-9.]{1,8}$").asPredicate();

    private static final Predicate<String> CURRENCY_PAIR_RAW_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9.]{1,8}[/-][a-zA-Z0-9.]{1,8}$").asPredicate();

    public static String transformCurrencyPair(String currencyPair) {
        if (!CURRENCY_PAIR_NAME_PATTERN.test(currencyPair)) {
            String message = String.format("Failed to parse currency pair name (%s) as expected: btc_usd", currencyPair);
            log.error(message);
            throw new CurrencyPairFormatException(message);
        }
        return currencyPair.replace('_', '/').replace('-', '/').toUpperCase();
    }

    /*todo tests*/
    public static String transformCurrencyPairBack(String currencyPair) {
        if (!CURRENCY_PAIR_RAW_NAME_PATTERN.test(currencyPair)) {
            String message = String.format("Failed to parse currency pair name (%s) as expected: btc/usd", currencyPair);
            log.error(message);
            throw new CurrencyPairFormatException(message);
        }
        return currencyPair.replace('/', '_').toLowerCase();
    }
}
