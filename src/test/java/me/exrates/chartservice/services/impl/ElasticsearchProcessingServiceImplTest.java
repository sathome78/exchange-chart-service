package me.exrates.chartservice.services.impl;

import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
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
//        List<CandleModel> candleModels = processingService.get(FROM_DATE, TO_DATE, BTC_USD);
//
//        assertNotNull(candleModels);
//        assertTrue(candleModels.isEmpty());
//
//        processingService.send();
    }
}
