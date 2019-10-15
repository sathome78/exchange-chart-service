package me.exrates.chartservice.configuration;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.cache.Cache;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@EnableCaching
@Configuration
public class CacheConfiguration {

    public final static String CURRENCY_PAIRS_CACHE = "cache.currency-pairs";
    public final static String CURRENCY_RATES_CACHE = "cache.currency-rates";

    @Bean(CURRENCY_PAIRS_CACHE)
    public Cache cacheCurrencyPairs() {
        return new CaffeineCache(CURRENCY_PAIRS_CACHE, Caffeine.newBuilder()
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build());
    }

    @Bean(CURRENCY_RATES_CACHE)
    public Cache cacheCurrencyRates() {
        return new CaffeineCache(CURRENCY_RATES_CACHE, Caffeine.newBuilder()
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .build());
    }
}