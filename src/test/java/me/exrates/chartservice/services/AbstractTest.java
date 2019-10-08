package me.exrates.chartservice.services;

import me.exrates.chartservice.configuration.CommonConfiguration;
import me.exrates.chartservice.configuration.RedisConfiguration;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.enums.IntervalType;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;
import java.util.List;

import static me.exrates.chartservice.configuration.CacheConfiguration.CURRENCY_PAIRS_CACHE;
import static me.exrates.chartservice.configuration.CacheConfiguration.CURRENCY_RATES_CACHE;
import static me.exrates.chartservice.configuration.CommonConfiguration.ALL_SUPPORTED_INTERVALS_LIST;

@RunWith(SpringRunner.class)
@ActiveProfiles("local")
@SpringBootTest(
        properties = {
                "candles.store-in-cache=300"
        },
        classes = {
                CommonConfiguration.class,
                RedisConfiguration.class
        })
public abstract class AbstractTest {

    static final String TEST_PAIR = "COIN1/COIN2";
    static final LocalDateTime NOW = LocalDateTime.now();
    static final int CANDLES_TO_STORE = 300;
    static final String BTC_USD = "BTC/USD";
    static final String BTC_USDT = "BTC/USDT";
    static final BackDealInterval M5_INTERVAL = new BackDealInterval(5, IntervalType.MINUTE);
    static final BackDealInterval M15_INTERVAL = new BackDealInterval(15, IntervalType.MINUTE);
    static final BackDealInterval M30_INTERVAL = new BackDealInterval(30, IntervalType.MINUTE);
    static final BackDealInterval ONE_HOUR_INTERVAL = new BackDealInterval(1, IntervalType.HOUR);
    static final BackDealInterval SIX_HOUR_INTERVAL = new BackDealInterval(6, IntervalType.HOUR);
    static final BackDealInterval ONE_DAY_INTERVAL = new BackDealInterval(1, IntervalType.DAY);

    @Value("${candles.store-in-cache:300}")
    long candlesToStoreInCache;

    @Autowired
    @Qualifier(ALL_SUPPORTED_INTERVALS_LIST)
    List<BackDealInterval> supportedIntervals;

    @Autowired
    @Qualifier(CURRENCY_PAIRS_CACHE)
    Cache currencyPairsCache;

    @Autowired
    @Qualifier(CURRENCY_RATES_CACHE)
    Cache currencyRatesCache;
}