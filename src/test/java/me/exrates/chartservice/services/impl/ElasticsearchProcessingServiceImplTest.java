package me.exrates.chartservice.services.impl;

import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@ActiveProfiles("local")
@SpringBootTest
public class ElasticsearchProcessingServiceImplTest {

    private static final LocalDateTime NOW = LocalDateTime.now();
    private static final LocalDateTime FROM_DATE = NOW.minusDays(1);
    private static final LocalDateTime TO_DATE = NOW.plusDays(1);
    private static final String BTC_USD = "BTC/USD";

    @Autowired
    private ElasticsearchProcessingService processingService;

    @Test
    public void endToEnd() {
        boolean exist = processingService.exist(BTC_USD, NOW);

        assertFalse(exist);

        CandleModel candleModel = CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW)
                .build();

        processingService.insert(candleModel, BTC_USD);

        exist = processingService.exist(BTC_USD, NOW);

        assertTrue(exist);

        CandleModel insertedCandleModel = processingService.get(BTC_USD, NOW);

        assertNotNull(insertedCandleModel);
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getOpenRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getCloseRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getHighRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getLowRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedCandleModel.getVolume()));
        assertEquals(NOW, insertedCandleModel.getCandleOpenTime());

        candleModel = CandleModel.builder()
                .openRate(BigDecimal.ZERO)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.ONE)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW)
                .build();

        processingService.update(candleModel, BTC_USD);

        CandleModel updatedCandleModel = processingService.get(BTC_USD, NOW);

        assertNotNull(updatedCandleModel);
        assertEquals(0, BigDecimal.ZERO.compareTo(updatedCandleModel.getOpenRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedCandleModel.getCloseRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedCandleModel.getHighRate()));
        assertEquals(0, BigDecimal.ONE.compareTo(updatedCandleModel.getLowRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedCandleModel.getVolume()));
        assertEquals(NOW, updatedCandleModel.getCandleOpenTime());

        List<CandleModel> models = processingService.getByQuery(FROM_DATE, TO_DATE, BTC_USD);

        assertNotNull(models);
        assertFalse(models.isEmpty());
        assertEquals(1, models.size());

        candleModel = CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW.plusMinutes(5))
                .build();

        processingService.insert(candleModel, BTC_USD);

        candleModel = CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW.plusDays(10))
                .build();

        processingService.insert(candleModel, BTC_USD);

        models = processingService.getByQuery(FROM_DATE, TO_DATE, BTC_USD);

        assertNotNull(models);
        assertFalse(models.isEmpty());
        assertEquals(2, models.size());

        long deletedCount = processingService.deleteAll();

        assertEquals(2L, deletedCount);
    }
}