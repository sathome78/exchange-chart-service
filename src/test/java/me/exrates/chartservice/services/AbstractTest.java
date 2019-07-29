package me.exrates.chartservice.services;

import com.antkorwin.xsync.XSync;
import me.exrates.chartservice.configuration.CommonConfiguration;
import me.exrates.chartservice.configuration.RedisConfiguration;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.enums.IntervalType;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;

import static me.exrates.chartservice.configuration.CommonConfiguration.ALL_SUPPORTED_INTERVALS_LIST;
import static me.exrates.chartservice.configuration.RedisConfiguration.NEXT_INTERVAL_MAP;

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
    static final int CANDLES_TO_STORE = 300;
    static final String BTC_USD = "btc_usd";
    static final String BTC_USDT = "btc_usdt";
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
    @Qualifier(NEXT_INTERVAL_MAP)
    Map<String, String> nextIntervalMap;
    @Autowired
    XSync<String> xSync;
}