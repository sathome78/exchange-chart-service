package me.exrates.chartservice.integrations;

import me.exrates.chartservice.RetryRule;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RedisProcessingServiceTestIT extends AbstractTestIT {

    private static final BackDealInterval DEFAULT_INTERVAL = new BackDealInterval(5, IntervalType.MINUTE);

    @Autowired
    private RedisProcessingService processingService;

    private String key;
    private String hashKey;

    @Rule
    public RetryRule retryRule = new RetryRule(3);

    @Before
    public void setUp() throws Exception {
        key = RedisGeneratorUtil.generateKey(NOW.toLocalDate());
        hashKey = RedisGeneratorUtil.generateHashKey(TEST_PAIR);
    }

    @After
    public void tearDown() throws Exception {
        processingService.deleteKey(key);
    }

    @Test
    public void endToEnd() {
        boolean exists = processingService.exists(key, DEFAULT_INTERVAL);

        assertFalse(exists);

        CandleModel model = CandleModel.builder()
                .pairName(TEST_PAIR)
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW)
                .build();

        List<CandleModel> models = Collections.singletonList(model);

        processingService.insertOrUpdate(models, key, hashKey, DEFAULT_INTERVAL);

        exists = processingService.exists(key, DEFAULT_INTERVAL);

        assertTrue(exists);

        List<CandleModel> insertedModels = processingService.get(key, hashKey, DEFAULT_INTERVAL);

        assertNotNull(insertedModels);
        assertFalse(insertedModels.isEmpty());

        CandleModel insertedModel = insertedModels.get(0);

        assertEquals(0, BigDecimal.TEN.compareTo(insertedModel.getOpenRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedModel.getCloseRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedModel.getHighRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedModel.getLowRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedModel.getVolume()));
        assertEquals(NOW, insertedModel.getCandleOpenTime());

        model = CandleModel.builder()
                .pairName(TEST_PAIR)
                .openRate(BigDecimal.ZERO)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.ONE)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW)
                .build();

        boolean replaced = Collections.replaceAll(insertedModels, insertedModel, model);

        assertTrue(replaced);

        processingService.insertOrUpdate(insertedModels, key, hashKey, DEFAULT_INTERVAL);

        List<CandleModel> updatedModels = processingService.get(key, hashKey, DEFAULT_INTERVAL);

        assertNotNull(updatedModels);
        assertFalse(updatedModels.isEmpty());

        CandleModel updatedModel = updatedModels.get(0);

        assertEquals(0, BigDecimal.ZERO.compareTo(updatedModel.getOpenRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedModel.getCloseRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedModel.getHighRate()));
        assertEquals(0, BigDecimal.ONE.compareTo(updatedModel.getLowRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedModel.getVolume()));
        assertEquals(NOW, updatedModel.getCandleOpenTime());

        CandleModel model1 = CandleModel.builder()
                .pairName(TEST_PAIR)
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW.minusMinutes(5))
                .build();

        CandleModel model2 = CandleModel.builder()
                .pairName(TEST_PAIR)
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW.plusDays(10))
                .build();

        updatedModels.add(model1);
        updatedModels.add(model2);

        Map<String, List<CandleModel>> mapOfModels = new HashMap<>();
        mapOfModels.put(key, updatedModels);

        processingService.bulkInsertOrUpdate(mapOfModels, hashKey, DEFAULT_INTERVAL);

        models = processingService.get(key, hashKey, DEFAULT_INTERVAL);

        assertFalse(CollectionUtils.isEmpty(models));
        assertEquals(3, models.size());

        LocalDateTime lastCandleTime = processingService.getLastCandleTimeBeforeDate(NOW, NOW.minusDays(1), hashKey, DEFAULT_INTERVAL);

        assertNotNull(lastCandleTime);

        List<String> keys = processingService.getAllKeys(DEFAULT_INTERVAL);

        assertFalse(CollectionUtils.isEmpty(keys));

        processingService.deleteDataByHashKey(key, hashKey, DEFAULT_INTERVAL);
    }

    @Test
    public void insertLastInitializedCandleTimeToCacheEndToEnd() {
        LocalDateTime dateTimeWithoutNano = NOW.withNano(0);

        processingService.insertLastInitializedCandleTimeToCache(hashKey, dateTimeWithoutNano);

        LocalDateTime dateTime = processingService.getLastInitializedCandleTimeFromCache(hashKey);

        assertNotNull(dateTime);
        assertEquals(dateTimeWithoutNano, dateTime);

        dateTimeWithoutNano = NOW.plusDays(1).withNano(0);

        processingService.insertLastInitializedCandleTimeToCache(hashKey, dateTimeWithoutNano);

        dateTime = processingService.getLastInitializedCandleTimeFromCache(hashKey);

        assertNotNull(dateTime);
        assertEquals(dateTimeWithoutNano, dateTime);

        processingService.deleteKeyByDbIndexAndKey(0, hashKey);
    }

    @Test
    public void insertFirstInitializedCandleTimeToHistoryEndToEnd() {
        LocalDateTime dateTimeWithoutNano = NOW.withNano(0);

        processingService.insertFirstInitializedCandleTimeToHistory(hashKey, dateTimeWithoutNano);

        LocalDateTime dateTime = processingService.getFirstInitializedCandleTimeFromHistory(hashKey);

        assertNotNull(dateTime);
        assertEquals(dateTimeWithoutNano, dateTime);

        dateTimeWithoutNano = NOW.plusDays(1).withNano(0);

        processingService.insertFirstInitializedCandleTimeToHistory(hashKey, dateTimeWithoutNano);

        dateTime = processingService.getFirstInitializedCandleTimeFromHistory(hashKey);

        assertNotNull(dateTime);
        assertEquals(dateTimeWithoutNano, dateTime);

        processingService.deleteKeyByDbIndexAndKey(0, hashKey);
    }
}