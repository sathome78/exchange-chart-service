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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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

    private String index;
    private String id;

    private String key;
    private String hashKey;

    @Rule
    public RetryRule retryRule = new RetryRule(3);

    @Before
    public void setUp() throws Exception {
        index = ElasticsearchGeneratorUtil.generateIndex(NOW.toLocalDate());
        id = ElasticsearchGeneratorUtil.generateId(TEST_PAIR);

        key = RedisGeneratorUtil.generateKey(NOW.toLocalDate());
        hashKey = RedisGeneratorUtil.generateHashKey(TEST_PAIR);
    }

    @After
    public void tearDown() throws Exception {
        // clear elasticsearch cluster

        elasticsearchProcessingService.deleteIndex(index);

        // clear redis cache

        redisProcessingService.deleteKey(index);

        redisProcessingService.deleteKeyByDbIndexAndKey(0, index);
    }

    @Test
    public void updateCache_ok() throws Exception {
        //initialize data in elasticsearch cluster

        String createdIndex = elasticsearchProcessingService.createIndex(index);

        assertNotNull(createdIndex);
        assertEquals(index, createdIndex);

        CandleModel model = CandleModel.builder()
                .pairName(TEST_PAIR)
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, FIVE_MINUTE_INTERVAL))
                .build();

        List<CandleModel> models = Collections.singletonList(model);

        elasticsearchProcessingService.insert(models, index, id);

        TimeUnit.SECONDS.sleep(1);

        //update redis cache for all available intervals

        cacheDataInitializerService.updateCacheByIndexAndId(index, id);

        TimeUnit.SECONDS.sleep(1);

        //check data from redis cache for all intervals

        Map<String, List<CandleModel>> mapOfModels = redisProcessingService.getAllByKey(index, FIVE_MINUTE_INTERVAL);

        assertNotNull(mapOfModels);

        Collection<List<CandleModel>> modelsCollection = mapOfModels.values();

        assertNotNull(modelsCollection);
        assertFalse(modelsCollection.isEmpty());

        models = modelsCollection.iterator().next();

        assertFalse(CollectionUtils.isEmpty(models));
        assertEquals(1, models.size());
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, FIVE_MINUTE_INTERVAL), models.get(0).getCandleOpenTime());

        mapOfModels = redisProcessingService.getAllByKey(index, FIFTEEN_MINUTE_INTERVAL);

        assertNotNull(mapOfModels);

        modelsCollection = mapOfModels.values();

        assertNotNull(modelsCollection);
        assertFalse(modelsCollection.isEmpty());

        models = modelsCollection.iterator().next();

        assertFalse(CollectionUtils.isEmpty(models));
        assertEquals(1, models.size());
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, FIFTEEN_MINUTE_INTERVAL), models.get(0).getCandleOpenTime());

        mapOfModels = redisProcessingService.getAllByKey(index, THIRTY_MINUTE_INTERVAL);

        assertNotNull(mapOfModels);

        modelsCollection = mapOfModels.values();

        assertNotNull(modelsCollection);
        assertFalse(modelsCollection.isEmpty());

        models = modelsCollection.iterator().next();

        assertFalse(CollectionUtils.isEmpty(models));
        assertEquals(1, models.size());
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, THIRTY_MINUTE_INTERVAL), models.get(0).getCandleOpenTime());

        mapOfModels = redisProcessingService.getAllByKey(index, ONE_HOUR_INTERVAL);

        assertNotNull(mapOfModels);

        modelsCollection = mapOfModels.values();

        assertNotNull(modelsCollection);
        assertFalse(modelsCollection.isEmpty());

        models = modelsCollection.iterator().next();

        assertFalse(CollectionUtils.isEmpty(models));
        assertEquals(1, models.size());
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, ONE_HOUR_INTERVAL), models.get(0).getCandleOpenTime());

        mapOfModels = redisProcessingService.getAllByKey(index, SIX_HOUR_INTERVAL);

        assertNotNull(mapOfModels);

        modelsCollection = mapOfModels.values();

        assertNotNull(modelsCollection);
        assertFalse(modelsCollection.isEmpty());

        models = modelsCollection.iterator().next();

        assertFalse(CollectionUtils.isEmpty(models));
        assertEquals(1, models.size());
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, SIX_HOUR_INTERVAL), models.get(0).getCandleOpenTime());

        mapOfModels = redisProcessingService.getAllByKey(index, ONE_DAY_INTERVAL);

        assertNotNull(mapOfModels);

        modelsCollection = mapOfModels.values();

        assertNotNull(modelsCollection);
        assertFalse(modelsCollection.isEmpty());

        models = modelsCollection.iterator().next();

        assertFalse(CollectionUtils.isEmpty(models));
        assertEquals(1, models.size());
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, ONE_DAY_INTERVAL), models.get(0).getCandleOpenTime());
    }

    @Test
    public void cleaneCache_ok_with_insert() throws Exception {
        //initialize data in redis cache

        CandleModel model = CandleModel.builder()
                .pairName(TEST_PAIR)
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(TimeUtil.getNearestBackTimeForBackdealInterval(NOW.minusDays(2), FIVE_MINUTE_INTERVAL))
                .build();

        List<CandleModel> models = Collections.singletonList(model);

        key = RedisGeneratorUtil.generateKey(NOW.minusDays(5).toLocalDate());

        redisProcessingService.insertOrUpdate(models, key, hashKey, FIVE_MINUTE_INTERVAL);

        //clean redis cache for all available intervals

        cacheDataInitializerService.cleanCache(FIVE_MINUTE_INTERVAL);

        TimeUnit.SECONDS.sleep(5);

        //check data from redis cache for five minute interval (should be empty)

        Map<String, List<CandleModel>> mapOfModels = redisProcessingService.getAllByKey(key, FIVE_MINUTE_INTERVAL);

        assertNotNull(mapOfModels);

        Collection<List<CandleModel>> modelsCollection = mapOfModels.values();

        assertNotNull(modelsCollection);
        assertTrue(modelsCollection.isEmpty());

        //check data from elasticsearch cluster (should not be empty)

        mapOfModels = elasticsearchProcessingService.getAllByIndex(key);

        assertNotNull(mapOfModels);

        modelsCollection = mapOfModels.values();

        assertNotNull(modelsCollection);
        assertFalse(modelsCollection.isEmpty());

        models = modelsCollection.iterator().next();

        assertFalse(CollectionUtils.isEmpty(models));
        assertEquals(1, models.size());
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW.minusDays(2), FIVE_MINUTE_INTERVAL), models.get(0).getCandleOpenTime());
    }

    @Test
    public void cleaneCache_ok_with_update() throws Exception {
        //initialize data in redis cache

        CandleModel model = CandleModel.builder()
                .pairName(TEST_PAIR)
                .openRate(BigDecimal.ONE)
                .closeRate(BigDecimal.ONE)
                .highRate(BigDecimal.ONE)
                .lowRate(BigDecimal.ONE)
                .volume(BigDecimal.ONE)
                .lastTradeTime(NOW.minusMinutes(100))
                .candleOpenTime(TimeUtil.getNearestBackTimeForBackdealInterval(NOW.minusDays(2), FIVE_MINUTE_INTERVAL))
                .build();

        List<CandleModel> models = Collections.singletonList(model);

        index = ElasticsearchGeneratorUtil.generateIndex(NOW.minusDays(5).toLocalDate());

        elasticsearchProcessingService.insert(models, index, id);

        model = CandleModel.builder()
                .pairName(TEST_PAIR)
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .lastTradeTime(NOW.minusMinutes(50))
                .candleOpenTime(TimeUtil.getNearestBackTimeForBackdealInterval(NOW.minusDays(2), FIVE_MINUTE_INTERVAL))
                .build();

        models = Collections.singletonList(model);

        key = RedisGeneratorUtil.generateKey(NOW.minusDays(5).toLocalDate());

        redisProcessingService.insertOrUpdate(models, key, hashKey, FIVE_MINUTE_INTERVAL);

        //clean redis cache for all available intervals

        cacheDataInitializerService.cleanCache(FIVE_MINUTE_INTERVAL);

        TimeUnit.SECONDS.sleep(5);

        //check data from redis cache for five minute interval (should be empty)

        Map<String, List<CandleModel>> mapOfModels = redisProcessingService.getAllByKey(key, FIVE_MINUTE_INTERVAL);

        assertNotNull(mapOfModels);

        Collection<List<CandleModel>> modelsCollection = mapOfModels.values();

        assertNotNull(modelsCollection);
        assertTrue(modelsCollection.isEmpty());

        //check data from elasticsearch cluster (should not be empty)

        mapOfModels = elasticsearchProcessingService.getAllByIndex(key);

        assertNotNull(mapOfModels);

        modelsCollection = mapOfModels.values();

        assertNotNull(modelsCollection);
        assertFalse(modelsCollection.isEmpty());

        models = modelsCollection.iterator().next();

        assertFalse(CollectionUtils.isEmpty(models));
        assertEquals(1, models.size());

        model = models.get(0);
        assertEquals(TimeUtil.getNearestBackTimeForBackdealInterval(NOW.minusDays(2), FIVE_MINUTE_INTERVAL), model.getCandleOpenTime());
        assertEquals(NOW.minusMinutes(50), model.getLastTradeTime());
        assertEquals(0, BigDecimal.TEN.compareTo(model.getOpenRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(model.getCloseRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(model.getHighRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(model.getLowRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(model.getVolume()));
    }
}