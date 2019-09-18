package me.exrates.chartservice.integrations;

import me.exrates.chartservice.RetryRule;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.utils.ElasticsearchGeneratorUtil;
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
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ElasticsearchProcessingServiceTestIT extends AbstractTestIT {

    @Autowired
    private ElasticsearchProcessingService processingService;

    private String index;
    private String id;

    @Rule
    public RetryRule retryRule = new RetryRule(3);

    @Before
    public void setUp() throws Exception {
        index = ElasticsearchGeneratorUtil.generateIndex(NOW.toLocalDate());
        id = ElasticsearchGeneratorUtil.generateId(TEST_PAIR);
    }

    @After
    public void tearDown() throws Exception {
        processingService.deleteIndex(index);
    }

    @Test
    public void endToEnd() throws Exception {
        boolean existsIndex = processingService.existsIndex(index);

        assertFalse(existsIndex);

        TimeUnit.SECONDS.sleep(1);

        String createdIndex = processingService.createIndex(index);

        assertNotNull(createdIndex);
        assertEquals(index, createdIndex);

        TimeUnit.SECONDS.sleep(1);

        existsIndex = processingService.existsIndex(index);

        assertTrue(existsIndex);

        TimeUnit.SECONDS.sleep(1);

        boolean exists = processingService.exists(index, id);

        assertFalse(exists);

        TimeUnit.SECONDS.sleep(1);

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

        processingService.insert(models, index, id);

        TimeUnit.SECONDS.sleep(1);

        exists = processingService.exists(index, id);

        assertTrue(exists);

        TimeUnit.SECONDS.sleep(1);

        List<CandleModel> insertedModels = processingService.get(index, id);

        assertNotNull(insertedModels);
        assertFalse(insertedModels.isEmpty());

        CandleModel insertedModel = insertedModels.get(0);

        assertNotNull(insertedModel);
        assertEquals(TEST_PAIR, insertedModel.getPairName());
        assertEquals(0, BigDecimal.TEN.compareTo(insertedModel.getOpenRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedModel.getCloseRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedModel.getHighRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedModel.getLowRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(insertedModel.getVolume()));
        assertEquals(NOW, insertedModel.getCandleOpenTime());

        TimeUnit.SECONDS.sleep(1);

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

        processingService.update(insertedModels, index, id);

        TimeUnit.SECONDS.sleep(1);

        List<CandleModel> updatedModels = processingService.get(index, id);

        assertNotNull(updatedModels);
        assertFalse(updatedModels.isEmpty());

        CandleModel updatedModel = updatedModels.get(0);

        assertEquals(TEST_PAIR, updatedModel.getPairName());
        assertEquals(0, BigDecimal.ZERO.compareTo(updatedModel.getOpenRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedModel.getCloseRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedModel.getHighRate()));
        assertEquals(0, BigDecimal.ONE.compareTo(updatedModel.getLowRate()));
        assertEquals(0, BigDecimal.TEN.compareTo(updatedModel.getVolume()));
        assertEquals(NOW, updatedModel.getCandleOpenTime());

        TimeUnit.SECONDS.sleep(1);

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
        mapOfModels.put(index, updatedModels);

        processingService.bulkInsertOrUpdate(mapOfModels, id);

        TimeUnit.SECONDS.sleep(1);

        models = processingService.get(index, id);

        assertFalse(CollectionUtils.isEmpty(models));
        assertEquals(3, models.size());

        TimeUnit.SECONDS.sleep(1);

        LocalDateTime lastCandleTime = processingService.getLastCandleTimeBeforeDate(NOW, NOW.minusDays(1), id);

        assertNotNull(lastCandleTime);

        TimeUnit.SECONDS.sleep(1);

        List<String> indices = processingService.getAllIndices();

        assertFalse(CollectionUtils.isEmpty(indices));

        TimeUnit.SECONDS.sleep(1);

        long deletedCount = processingService.deleteDataByIndex(index);

        assertEquals(1L, deletedCount);
    }
}