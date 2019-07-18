package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.impl.CacheDataInitializerServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CacheDataInitializerServiceTest extends AbstractTest {

    private static final LocalDateTime NOW = LocalDateTime.now();

    @Mock
    private ElasticsearchProcessingService elasticsearchProcessingService;
    @Mock
    private RedisProcessingService redisProcessingService;
    @Mock
    private TradeDataService tradeDataService;

    private CacheDataInitializerService cacheDataInitializerService;

    @Before
    public void setUp() throws Exception {
        cacheDataInitializerService = spy(new CacheDataInitializerServiceImpl(
                elasticsearchProcessingService,
                redisProcessingService,
                tradeDataService,
                candlesToStoreInCache,
                supportedIntervals,
                nextIntervalMap));
    }

    @Test
    public void updateCache_ok() {
        doReturn(Collections.singletonList("btc_usd"))
                .when(elasticsearchProcessingService)
                .getAllIndices();
        doReturn(false)
                .when(redisProcessingService)
                .exists(anyString(), any(BackDealInterval.class));
        doReturn(Collections.singletonList(CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW)
                .build()))
                .when(elasticsearchProcessingService)
                .getAllByIndex(anyString());
        doNothing()
                .when(tradeDataService)
                .defineAndSaveLastInitializedCandleTime(anyString(), anyList());
        doNothing()
                .when(redisProcessingService)
                .batchInsertOrUpdate(anyList(), anyString(), any(BackDealInterval.class));

        cacheDataInitializerService.updateCache();

        verify(elasticsearchProcessingService, atLeastOnce()).getAllIndices();
        verify(redisProcessingService, atLeastOnce()).exists(anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, atLeastOnce()).getAllByIndex(anyString());
        verify(tradeDataService, atLeastOnce()).defineAndSaveLastInitializedCandleTime(anyString(), anyList());
        verify(redisProcessingService, times(6)).batchInsertOrUpdate(anyList(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void updateCache_indices_list_empty() {
        doReturn(Collections.emptyList())
                .when(elasticsearchProcessingService)
                .getAllIndices();

        cacheDataInitializerService.updateCache();

        verify(elasticsearchProcessingService, atLeastOnce()).getAllIndices();
        verify(redisProcessingService, never()).exists(anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, never()).getAllByIndex(anyString());
        verify(tradeDataService, never()).defineAndSaveLastInitializedCandleTime(anyString(), anyList());
        verify(redisProcessingService, never()).batchInsertOrUpdate(anyList(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void updateCache_index_present_in_cache() {
        doReturn(Collections.singletonList("btc_usd"))
                .when(elasticsearchProcessingService)
                .getAllIndices();
        doReturn(true)
                .when(redisProcessingService)
                .exists(anyString(), any(BackDealInterval.class));

        cacheDataInitializerService.updateCache();

        verify(elasticsearchProcessingService, atLeastOnce()).getAllIndices();
        verify(redisProcessingService, atLeastOnce()).exists(anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, never()).getAllByIndex(anyString());
        verify(tradeDataService, never()).defineAndSaveLastInitializedCandleTime(anyString(), anyList());
        verify(redisProcessingService, never()).batchInsertOrUpdate(anyList(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void updateCache_empty_data_list() {
        doReturn(Collections.singletonList("btc_usd"))
                .when(elasticsearchProcessingService)
                .getAllIndices();
        doReturn(false)
                .when(redisProcessingService)
                .exists(anyString(), any(BackDealInterval.class));
        doReturn(Collections.emptyList())
                .when(elasticsearchProcessingService)
                .getAllByIndex(anyString());

        cacheDataInitializerService.updateCache();

        verify(elasticsearchProcessingService, atLeastOnce()).getAllIndices();
        verify(redisProcessingService, atLeastOnce()).exists(anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, atLeastOnce()).getAllByIndex(anyString());
        verify(tradeDataService, never()).defineAndSaveLastInitializedCandleTime(anyString(), anyList());
        verify(redisProcessingService, never()).batchInsertOrUpdate(anyList(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void cleanCache_ok() {
        doReturn(Collections.singletonList("btc_usd"))
                .when(redisProcessingService)
                .getAllKeys(any(BackDealInterval.class));
        doReturn(Collections.singletonList(CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW.minusDays(2))
                .build()))
                .when(redisProcessingService)
                .getAllByKey(anyString(), any(BackDealInterval.class));
        doReturn(false)
                .when(elasticsearchProcessingService)
                .exists(anyString(), anyString());
        doNothing()
                .when(elasticsearchProcessingService)
                .insert(any(CandleModel.class), anyString());
        doNothing()
                .when(redisProcessingService)
                .deleteByHashKey(anyString(), anyString(), any(BackDealInterval.class));

        cacheDataInitializerService.cleanCache();

        verify(redisProcessingService, times(6)).getAllKeys(any(BackDealInterval.class));
        verify(redisProcessingService, times(6)).getAllByKey(anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, atLeastOnce()).exists(anyString(), anyString());
        verify(elasticsearchProcessingService, atLeastOnce()).insert(any(CandleModel.class), anyString());
        verify(redisProcessingService, atLeastOnce()).deleteByHashKey(anyString(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void cleanCache_key_lists_empty() {
        doReturn(Collections.emptyList())
                .when(redisProcessingService)
                .getAllKeys(any(BackDealInterval.class));

        cacheDataInitializerService.cleanCache();

        verify(redisProcessingService, times(6)).getAllKeys(any(BackDealInterval.class));
        verify(redisProcessingService, never()).getAllByKey(anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, never()).exists(anyString(), anyString());
        verify(elasticsearchProcessingService, never()).insert(any(CandleModel.class), anyString());
        verify(redisProcessingService, never()).deleteByHashKey(anyString(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void cleanCache_empty_data_list() {
        doReturn(Collections.singletonList("btc_usd"))
                .when(redisProcessingService)
                .getAllKeys(any(BackDealInterval.class));
        doReturn(Collections.emptyList())
                .when(redisProcessingService)
                .getAllByKey(anyString(), any(BackDealInterval.class));

        cacheDataInitializerService.cleanCache();

        verify(redisProcessingService, times(6)).getAllKeys(any(BackDealInterval.class));
        verify(redisProcessingService, times(6)).getAllByKey(anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, never()).exists(anyString(), anyString());
        verify(elasticsearchProcessingService, never()).insert(any(CandleModel.class), anyString());
        verify(redisProcessingService, never()).deleteByHashKey(anyString(), anyString(), any(BackDealInterval.class));
    }
}