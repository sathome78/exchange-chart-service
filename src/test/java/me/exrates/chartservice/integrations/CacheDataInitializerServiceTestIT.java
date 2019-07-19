package me.exrates.chartservice.integrations;

import me.exrates.chartservice.RetryRule;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.CacheDataInitializerService;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.utils.ElasticsearchGeneratorUtil;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CacheDataInitializerServiceTestIT extends AbstractTestIT {

    private static final BackDealInterval FIVE_MINUTE_INTERVAL = new BackDealInterval(5, IntervalType.MINUTE);
    private static final BackDealInterval FIFTEEN_MINUTE_INTERVAL = new BackDealInterval(15, IntervalType.MINUTE);
    private static final BackDealInterval THIRTY_MINUTE_INTERVAL = new BackDealInterval(30, IntervalType.MINUTE);
    private static final BackDealInterval ONE_HOUR_INTERVAL = new BackDealInterval(1, IntervalType.HOUR);
    private static final BackDealInterval SIX_HOUR_INTERVAL = new BackDealInterval(6, IntervalType.HOUR);
    private static final BackDealInterval ONE_DAY_INTERVAL = new BackDealInterval(1, IntervalType.DAY);

    @Autowired
    private ElasticsearchProcessingService elasticsearchProcessingService;
    @Autowired
    private RedisProcessingService redisProcessingService;
    @Autowired
    private CacheDataInitializerService cacheDataInitializerService;

    @Rule
    public RetryRule retryRule = new RetryRule(3);

    @Test
    public void updateCache_ok() throws Exception {
        final String index = ElasticsearchGeneratorUtil.generateIndex(TEST_PAIR);

        //initialize data in elasticsearch cluster

        CandleModel candleModel = CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, FIVE_MINUTE_INTERVAL))
                .build();

        elasticsearchProcessingService.insert(candleModel, index);

        //update redis cache for all available intervals

        cacheDataInitializerService.updateCache();

        TimeUnit.SECONDS.sleep(5);

        //check data from redis cache for all intervals

        List<CandleModel> candles = redisProcessingService.getAllByKey(index, FIVE_MINUTE_INTERVAL);

        assertFalse(CollectionUtils.isEmpty(candles));
        assertEquals(1, candles.size());
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, FIVE_MINUTE_INTERVAL), candles.get(0).getCandleOpenTime());

        candles = redisProcessingService.getAllByKey(index, FIFTEEN_MINUTE_INTERVAL);

        assertFalse(CollectionUtils.isEmpty(candles));
        assertEquals(1, candles.size());
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, FIFTEEN_MINUTE_INTERVAL), candles.get(0).getCandleOpenTime());

        candles = redisProcessingService.getAllByKey(index, THIRTY_MINUTE_INTERVAL);

        assertFalse(CollectionUtils.isEmpty(candles));
        assertEquals(1, candles.size());
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, THIRTY_MINUTE_INTERVAL), candles.get(0).getCandleOpenTime());

        candles = redisProcessingService.getAllByKey(index, ONE_HOUR_INTERVAL);

        assertFalse(CollectionUtils.isEmpty(candles));
        assertEquals(1, candles.size());
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, ONE_HOUR_INTERVAL), candles.get(0).getCandleOpenTime());

        candles = redisProcessingService.getAllByKey(index, SIX_HOUR_INTERVAL);

        assertFalse(CollectionUtils.isEmpty(candles));
        assertEquals(1, candles.size());
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, SIX_HOUR_INTERVAL), candles.get(0).getCandleOpenTime());

        candles = redisProcessingService.getAllByKey(index, ONE_DAY_INTERVAL);

        assertFalse(CollectionUtils.isEmpty(candles));
        assertEquals(1, candles.size());
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, ONE_DAY_INTERVAL), candles.get(0).getCandleOpenTime());

        // clear elasticsearch cluster

        elasticsearchProcessingService.deleteAllData();

        elasticsearchProcessingService.deleteAllIndices();

        // clear redis cache

        redisProcessingService.deleteAll();
    }

    @Test
    public void cleaneCache_ok_with_insert() throws Exception {
        final String key = RedisGeneratorUtil.generateKey(TEST_PAIR);

        //initialize data in redis cache

        CandleModel candleModel = CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(TimeUtil.getNearestBackTimeForBackdealInterval(NOW.minusDays(2), FIVE_MINUTE_INTERVAL))
                .build();

        redisProcessingService.insertOrUpdate(candleModel, key, FIVE_MINUTE_INTERVAL);

        //clean redis cache for all available intervals

        cacheDataInitializerService.cleanCache();

        TimeUnit.SECONDS.sleep(5);

        //check data from redis cache for five minute interval (should be empty)

        List<CandleModel> candles = redisProcessingService.getAllByKey(key, FIVE_MINUTE_INTERVAL);

        assertTrue(CollectionUtils.isEmpty(candles));

        //check data from elasticsearch cluster (should not be empty)

        candles = elasticsearchProcessingService.getAllByIndex(key);

        assertFalse(CollectionUtils.isEmpty(candles));
        assertEquals(1, candles.size());
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW.minusDays(2), FIVE_MINUTE_INTERVAL), candles.get(0).getCandleOpenTime());

        // clear elasticsearch cluster

        elasticsearchProcessingService.deleteAllData();

        elasticsearchProcessingService.deleteAllIndices();

        // clear redis cache

        redisProcessingService.deleteAll();
    }

    @Test
    public void cleaneCache_ok_with_update() throws Exception {
        final String key = RedisGeneratorUtil.generateKey(TEST_PAIR);

        //initialize data in redis cache

        CandleModel candleModel = CandleModel.builder()
                .openRate(BigDecimal.ONE)
                .closeRate(BigDecimal.ONE)
                .highRate(BigDecimal.ONE)
                .lowRate(BigDecimal.ONE)
                .volume(BigDecimal.ONE)
                .lastTradeTime(NOW.minusMinutes(100))
                .candleOpenTime(TimeUtil.getNearestBackTimeForBackdealInterval(NOW.minusDays(2), FIVE_MINUTE_INTERVAL))
                .build();

        elasticsearchProcessingService.insert(candleModel, key);

        candleModel = CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .lastTradeTime(NOW.minusMinutes(50))
                .candleOpenTime(TimeUtil.getNearestBackTimeForBackdealInterval(NOW.minusDays(2), FIVE_MINUTE_INTERVAL))
                .build();

        redisProcessingService.insertOrUpdate(candleModel, key, FIVE_MINUTE_INTERVAL);

        //clean redis cache for all available intervals

        cacheDataInitializerService.cleanCache();

        TimeUnit.SECONDS.sleep(5);

        //check data from redis cache for five minute interval (should be empty)

        List<CandleModel> candles = redisProcessingService.getAllByKey(key, FIVE_MINUTE_INTERVAL);

        assertTrue(CollectionUtils.isEmpty(candles));

        //check data from elasticsearch cluster (should not be empty)

        candles = elasticsearchProcessingService.getAllByIndex(key);

        assertFalse(CollectionUtils.isEmpty(candles));
        assertEquals(1, candles.size());

        candleModel = candles.get(0);
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW.minusDays(2), FIVE_MINUTE_INTERVAL), candleModel.getCandleOpenTime());
        assertEquals(NOW.minusMinutes(50), candleModel.getLastTradeTime());
        assertEquals(0, BigDecimal.TEN.compareTo(candleModel.getOpenRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(candleModel.getCloseRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(candleModel.getHighRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(candleModel.getLowRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(candleModel.getVolume()));

        // clear elasticsearch cluster

        elasticsearchProcessingService.deleteAllData();

        elasticsearchProcessingService.deleteAllIndices();

        // clear redis cache

        redisProcessingService.deleteAll();
    }
}