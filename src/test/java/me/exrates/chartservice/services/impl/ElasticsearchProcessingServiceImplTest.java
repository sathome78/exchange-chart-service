package me.exrates.chartservice.services.impl;

import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ElasticsearchProcessingServiceImplTest extends AbstractTest {

    @Autowired
    private ElasticsearchProcessingService processingService;

    @Test
    public void endToEnd() throws Exception {
        boolean exist = processingService.exist(BTC_USD, NOW);

        assertFalse(exist);

        TimeUnit.SECONDS.sleep(1);

        CandleModel candleModel = CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW)
                .build();

        processingService.insert(candleModel, BTC_USD);

        TimeUnit.SECONDS.sleep(1);

        exist = processingService.exist(BTC_USD, NOW);

        assertTrue(exist);

        TimeUnit.SECONDS.sleep(1);

        CandleModel insertedCandleModel = processingService.get(BTC_USD, NOW);

        assertNotNull(insertedCandleModel);
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getOpenRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getCloseRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getHighRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getLowRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getVolume()));
        assertEquals(NOW, insertedCandleModel.getCandleOpenTime());
        assertEquals(Timestamp.valueOf(NOW).getTime(), insertedCandleModel.getTimeInMillis());

        TimeUnit.SECONDS.sleep(1);

        candleModel = CandleModel.builder()
                .openRate(BigDecimal.ZERO)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.ONE)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW)
                .build();

        processingService.update(candleModel, BTC_USD);

        TimeUnit.SECONDS.sleep(1);

        CandleModel updatedCandleModel = processingService.get(BTC_USD, NOW);

        assertNotNull(updatedCandleModel);
        assertEquals(0, BigDecimal.ZERO.compareTo(updatedCandleModel.getOpenRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedCandleModel.getCloseRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedCandleModel.getHighRate()));
        assertEquals(0, BigDecimal.ONE.compareTo(updatedCandleModel.getLowRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedCandleModel.getVolume()));
        assertEquals(NOW, updatedCandleModel.getCandleOpenTime());
        assertEquals(Timestamp.valueOf(NOW).getTime(), updatedCandleModel.getTimeInMillis());

        TimeUnit.SECONDS.sleep(1);

        List<CandleModel> models = processingService.getByRange(FROM_DATE, TO_DATE, BTC_USD);

        assertNotNull(models);
        assertFalse(models.isEmpty());
        assertEquals(1, models.size());

        TimeUnit.SECONDS.sleep(1);

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

        processingService.batchInsert(Arrays.asList(candleModel1, candleModel2), BTC_USD);

        TimeUnit.SECONDS.sleep(1);

        models = processingService.getByRange(FROM_DATE, TO_DATE, BTC_USD);

        assertNotNull(models);
        assertFalse(models.isEmpty());
        assertEquals(2, models.size());

        TimeUnit.SECONDS.sleep(1);

        long deletedCount = processingService.deleteAll();

        assertEquals(3L, deletedCount);
    }
}
