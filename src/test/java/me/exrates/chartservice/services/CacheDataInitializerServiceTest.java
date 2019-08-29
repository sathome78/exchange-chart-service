package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.impl.CacheDataInitializerServiceImpl;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.after;
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

    private String key;

    @Before
    public void setUp() throws Exception {
        key = RedisGeneratorUtil.generateKey(TEST_PAIR);

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
        doReturn(Collections.singletonList(key))
                .when(elasticsearchProcessingService)
                .getAllIndices();
        doReturn(Collections.singletonList(CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW)
                .build()))
                .when(elasticsearchProcessingService)
                .getByRange(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
        doNothing()
                .when(tradeDataService)
                .defineAndSaveLastInitializedCandleTime(anyString(), anyList());
        doReturn(null)
                .when(redisProcessingService)
                .get(anyString(), anyString(), any(BackDealInterval.class));
        doNothing()
                .when(redisProcessingService)
                .insertOrUpdate(any(CandleModel.class), anyString(), any(BackDealInterval.class));

        cacheDataInitializerService.updateCache();

        verify(elasticsearchProcessingService, atLeastOnce()).getAllIndices();
        verify(elasticsearchProcessingService, atLeastOnce()).getByRange(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
        verify(tradeDataService, atLeastOnce()).defineAndSaveLastInitializedCandleTime(anyString(), anyList());
        verify(redisProcessingService, times(6)).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, times(6)).insertOrUpdate(any(CandleModel.class), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void updateCache_indices_list_empty() {
        doReturn(Collections.emptyList())
                .when(elasticsearchProcessingService)
                .getAllIndices();

        cacheDataInitializerService.updateCache();

        verify(elasticsearchProcessingService, atLeastOnce()).getAllIndices();
        verify(elasticsearchProcessingService, never()).getAllByIndex(anyString());
        verify(tradeDataService, never()).defineAndSaveLastInitializedCandleTime(anyString(), anyList());
        verify(redisProcessingService, never()).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, never()).insertOrUpdate(any(CandleModel.class), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void updateCache_empty_data_list() {
        doReturn(Collections.singletonList(key))
                .when(elasticsearchProcessingService)
                .getAllIndices();
        doReturn(Collections.emptyList())
                .when(elasticsearchProcessingService)
                .getByRange(any(LocalDateTime.class), any(LocalDateTime.class), anyString());

        cacheDataInitializerService.updateCache();

        verify(elasticsearchProcessingService, atLeastOnce()).getAllIndices();
        verify(elasticsearchProcessingService, times(6)).getByRange(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
        verify(tradeDataService, never()).defineAndSaveLastInitializedCandleTime(anyString(), anyList());
        verify(redisProcessingService, never()).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, never()).insertOrUpdate(any(CandleModel.class), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void cleanCache_ok_with_insert() {
        doReturn(Collections.singletonList(key))
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
                .deleteDataByHashKey(anyString(), anyString(), any(BackDealInterval.class));
        doNothing()
                .when(tradeDataService)
                .defineAndSaveLastInitializedCandleTime(anyString(), anyList());

        cacheDataInitializerService.cleanCache();

        verify(redisProcessingService, times(6)).getAllKeys(any(BackDealInterval.class));
        verify(redisProcessingService, times(7)).getAllByKey(anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, atLeastOnce()).exists(anyString(), anyString());
        verify(elasticsearchProcessingService, atLeastOnce()).insert(any(CandleModel.class), anyString());
        verify(redisProcessingService, atLeastOnce()).deleteDataByHashKey(anyString(), anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, never()).get(anyString(), anyString());
        verify(elasticsearchProcessingService, never()).update(any(CandleModel.class), anyString());
        verify(tradeDataService, after(100)).defineAndSaveLastInitializedCandleTime(anyString(), anyList());
    }

    @Test
    public void cleanCache_ok_with_update() {
        doReturn(Collections.singletonList(key))
                .when(redisProcessingService)
                .getAllKeys(any(BackDealInterval.class));
        doReturn(Collections.singletonList(CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .lastTradeTime(NOW.minusMinutes(50))
                .candleOpenTime(NOW.minusDays(2))
                .build()))
                .when(redisProcessingService)
                .getAllByKey(anyString(), any(BackDealInterval.class));
        doReturn(true)
                .when(elasticsearchProcessingService)
                .exists(anyString(), anyString());
        doReturn(CandleModel.builder()
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .lastTradeTime(NOW.minusMinutes(100))
                .candleOpenTime(NOW.minusDays(2))
                .build())
                .when(elasticsearchProcessingService)
                .get(anyString(), anyString());
        doNothing()
                .when(elasticsearchProcessingService)
                .update(any(CandleModel.class), anyString());
        doNothing()
                .when(redisProcessingService)
                .deleteDataByHashKey(anyString(), anyString(), any(BackDealInterval.class));
        doNothing()
                .when(tradeDataService)
                .defineAndSaveLastInitializedCandleTime(anyString(), anyList());

        cacheDataInitializerService.cleanCache();

        verify(redisProcessingService, times(6)).getAllKeys(any(BackDealInterval.class));
        verify(redisProcessingService, times(7)).getAllByKey(anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, atLeastOnce()).exists(anyString(), anyString());
        verify(elasticsearchProcessingService, never()).insert(any(CandleModel.class), anyString());
        verify(redisProcessingService, atLeastOnce()).deleteDataByHashKey(anyString(), anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, atLeastOnce()).get(anyString(), anyString());
        verify(elasticsearchProcessingService, atLeastOnce()).update(any(CandleModel.class), anyString());
        verify(tradeDataService, after(100)).defineAndSaveLastInitializedCandleTime(anyString(), anyList());
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
        verify(redisProcessingService, never()).deleteDataByHashKey(anyString(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void cleanCache_empty_data_list() {
        doReturn(Collections.singletonList(key))
                .when(redisProcessingService)
                .getAllKeys(any(BackDealInterval.class));
        doReturn(Collections.emptyList())
                .when(redisProcessingService)
                .getAllByKey(anyString(), any(BackDealInterval.class));
        doNothing()
                .when(tradeDataService)
                .defineAndSaveLastInitializedCandleTime(anyString(), anyList());

        cacheDataInitializerService.cleanCache();

        verify(redisProcessingService, times(6)).getAllKeys(any(BackDealInterval.class));
        verify(redisProcessingService, times(7)).getAllByKey(anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, never()).exists(anyString(), anyString());
        verify(elasticsearchProcessingService, never()).insert(any(CandleModel.class), anyString());
        verify(redisProcessingService, never()).deleteDataByHashKey(anyString(), anyString(), any(BackDealInterval.class));
        verify(tradeDataService, after(100)).defineAndSaveLastInitializedCandleTime(anyString(), anyList());
    }
}