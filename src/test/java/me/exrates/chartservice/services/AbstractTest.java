package me.exrates.chartservice.services;

import me.exrates.chartservice.configuration.CommonConfiguration;
import me.exrates.chartservice.configuration.RedisConfiguration;
import me.exrates.chartservice.model.BackDealInterval;
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

    @Value("${candles.store-in-cache:300}")
    long candlesToStoreInCache;

    @Autowired
    @Qualifier(ALL_SUPPORTED_INTERVALS_LIST)
    List<BackDealInterval> supportedIntervals;
    @Autowired
    @Qualifier(NEXT_INTERVAL_MAP)
    Map<String, String> nextIntervalMap;
}