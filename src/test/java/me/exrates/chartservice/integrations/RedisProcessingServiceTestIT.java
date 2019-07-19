package me.exrates.chartservice.integrations;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RedisProcessingServiceTestIT extends AbstractTestIT {

    private static final BackDealInterval DEFAULT_INTERVAL = new BackDealInterval(5, IntervalType.MINUTE);

    @Autowired
    private RedisProcessingService processingService;

    @Test
    public void endToEnd() {
        final String key = RedisGeneratorUtil.generateKey(TEST_PAIR);
        final String hashKey = RedisGeneratorUtil.generateHashKey(NOW);

        boolean exists = processingService.exists(key, DEFAULT_INTERVAL);

        assertFalse(exists);

        CandleModel candleModel = CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW)
                .build();

        processingService.insertOrUpdate(candleModel, key, DEFAULT_INTERVAL);

        exists = processingService.exists(key, DEFAULT_INTERVAL);

        assertTrue(exists);

        CandleModel insertedCandleModel = processingService.get(key, hashKey, DEFAULT_INTERVAL);

        assertNotNull(insertedCandleModel);
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getOpenRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getCloseRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getHighRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getLowRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getVolume()));
        assertEquals(NOW, insertedCandleModel.getCandleOpenTime());
        assertEquals(Timestamp.valueOf(NOW).getTime(), insertedCandleModel.getTimeInMillis());

        candleModel = CandleModel.builder()
                .openRate(BigDecimal.ZERO)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.ONE)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW)
                .build();

        processingService.insertOrUpdate(candleModel, key, DEFAULT_INTERVAL);

        CandleModel updatedCandleModel = processingService.get(key, hashKey, DEFAULT_INTERVAL);

        assertNotNull(updatedCandleModel);
        assertEquals(0, BigDecimal.ZERO.compareTo(updatedCandleModel.getOpenRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedCandleModel.getCloseRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedCandleModel.getHighRate()));
        assertEquals(0, BigDecimal.ONE.compareTo(updatedCandleModel.getLowRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedCandleModel.getVolume()));
        assertEquals(NOW, updatedCandleModel.getCandleOpenTime());
        assertEquals(Timestamp.valueOf(NOW).getTime(), updatedCandleModel.getTimeInMillis());

        List<CandleModel> models = processingService.getByRange(FROM_DATE, TO_DATE, key, DEFAULT_INTERVAL);

        assertFalse(CollectionUtils.isEmpty(models));
        assertEquals(1, models.size());

        CandleModel candleModel1 = CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW.plusMinutes(5))
                .build();

        CandleModel candleModel2 = CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW.plusDays(10))
                .build();

        processingService.batchInsertOrUpdate(Arrays.asList(candleModel1, candleModel2), key, DEFAULT_INTERVAL);

        models = processingService.getAllByKey(key, DEFAULT_INTERVAL);

        assertFalse(CollectionUtils.isEmpty(models));
        assertEquals(3, models.size());

        models = processingService.getByRange(FROM_DATE, TO_DATE, key, DEFAULT_INTERVAL);

        assertFalse(CollectionUtils.isEmpty(models));
        assertEquals(2, models.size());

        List<String> keys = processingService.getAllKeys(DEFAULT_INTERVAL);

        assertFalse(CollectionUtils.isEmpty(keys));

        processingService.deleteByHashKey(key, hashKey, DEFAULT_INTERVAL);

        processingService.deleteAll();
    }

    @Test
    public void insertAndGetLastInitializedCandleTimeEndToEnd() {
        LocalDateTime dateTimeWithoutNano = NOW.withNano(0);

        processingService.insertLastInitializedCandleTimeToCache(TEST_PAIR, dateTimeWithoutNano);

        LocalDateTime dateTime = processingService.getLastInitializedCandleTimeFromCache(TEST_PAIR);

        assertNotNull(dateTime);
        assertEquals(dateTimeWithoutNano, dateTime);

        dateTimeWithoutNano = NOW.plusDays(1).withNano(0);

        processingService.insertLastInitializedCandleTimeToCache(TEST_PAIR, dateTimeWithoutNano);

        dateTime = processingService.getLastInitializedCandleTimeFromCache(TEST_PAIR);

        assertNotNull(dateTime);
        assertEquals(dateTimeWithoutNano, dateTime);

        processingService.deleteByDbIndex(0);
    }
}