package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.DailyDataModel;
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
    @Mock
    private OrderService orderService;

    private CacheDataInitializerService cacheDataInitializerService;

    private String key;
    private String hashKey;

    @Before
    public void setUp() throws Exception {
        key = RedisGeneratorUtil.generateKey(NOW.toLocalDate());
        hashKey = RedisGeneratorUtil.generateHashKey(TEST_PAIR);

        cacheDataInitializerService = spy(new CacheDataInitializerServiceImpl(
                elasticsearchProcessingService,
                redisProcessingService,
                tradeDataService,
                orderService,
                candlesToStoreInCache,
                supportedIntervals));
    }

    @Test
    public void updateCacheByIndexAndId_ok() {
        CandleModel model = CandleModel.builder()
                .pairName(TEST_PAIR)
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW)
                .currencyVolume(BigDecimal.TEN)
                .percentChange(BigDecimal.ZERO)
                .valueChange(BigDecimal.ZERO)
                .predLastRate(BigDecimal.ONE)
                .build();
        doReturn(Collections.singletonList(model))
                .when(elasticsearchProcessingService)
                .get(anyString(), anyString());
        doNothing()
                .when(tradeDataService)
                .defineAndSaveFirstInitializedCandleTime(anyString(), anyList());
        doNothing()
                .when(tradeDataService)
                .defineAndSaveLastInitializedCandleTime(anyString(), anyList());
        doReturn(false)
                .when(redisProcessingService)
                .exists(anyString(), anyString(), any(BackDealInterval.class));
        doNothing()
                .when(redisProcessingService)
                .insertOrUpdate(anyList(), anyString(), anyString(), any(BackDealInterval.class));

        cacheDataInitializerService.updateCacheByIndexAndId(key, hashKey);

        verify(elasticsearchProcessingService, atLeastOnce()).get(anyString(), anyString());
        verify(tradeDataService, after(100)).defineAndSaveFirstInitializedCandleTime(anyString(), anyList());
        verify(tradeDataService, after(100)).defineAndSaveLastInitializedCandleTime(anyString(), anyList());
        verify(redisProcessingService, times(6)).exists(anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, times(6)).insertOrUpdate(anyList(), anyString(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void updateCacheByIndexAndId_empty_data_list() {
        doReturn(Collections.emptyList())
                .when(elasticsearchProcessingService)
                .get(anyString(), anyString());
        doNothing()
                .when(tradeDataService)
                .defineAndSaveFirstInitializedCandleTime(anyString(), anyList());

        cacheDataInitializerService.updateCacheByIndexAndId(key, hashKey);

        verify(elasticsearchProcessingService, atLeastOnce()).get(anyString(), anyString());
        verify(tradeDataService, after(100)).defineAndSaveFirstInitializedCandleTime(anyString(), anyList());
        verify(tradeDataService, never()).defineAndSaveLastInitializedCandleTime(anyString(), anyList());
        verify(redisProcessingService, never()).exists(anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, never()).insertOrUpdate(anyList(), anyString(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void cleanCache_ok_with_insert() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .name(TEST_PAIR)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(null);

        key = RedisGeneratorUtil.generateKey(NOW.minusDays(10).toLocalDate());

        doReturn(Collections.singletonList(key))
                .when(redisProcessingService)
                .getAllKeys(any(BackDealInterval.class));

        CandleModel model = CandleModel.builder()
                .pairName(TEST_PAIR)
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(NOW.minusDays(2))
                .build();
        doReturn(Collections.singletonList(model))
                .when(redisProcessingService)
                .get(anyString(), anyString(), any(BackDealInterval.class));
        doReturn(false)
                .when(elasticsearchProcessingService)
                .exists(anyString(), anyString());
        doNothing()
                .when(elasticsearchProcessingService)
                .insert(anyList(), anyString(), anyString());
        doNothing()
                .when(redisProcessingService)
                .deleteDataByHashKey(anyString(), anyString(), any(BackDealInterval.class));

        cacheDataInitializerService.cleanCache();

        verify(orderService, times(6)).getCurrencyPairsFromCache(null);
        verify(redisProcessingService, times(6)).getAllKeys(any(BackDealInterval.class));
        verify(redisProcessingService, times(3)).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, atLeastOnce()).exists(anyString(), anyString());
        verify(elasticsearchProcessingService, atLeastOnce()).insert(anyList(), anyString(), anyString());
        verify(redisProcessingService, atLeastOnce()).deleteDataByHashKey(anyString(), anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, never()).update(anyList(), anyString(), anyString());
    }

    @Test
    public void cleanCache_ok_with_update() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .name(TEST_PAIR)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(null);

        key = RedisGeneratorUtil.generateKey(NOW.minusDays(10).toLocalDate());

        doReturn(Collections.singletonList(key))
                .when(redisProcessingService)
                .getAllKeys(any(BackDealInterval.class));

        CandleModel model = CandleModel.builder()
                .pairName(TEST_PAIR)
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .lastTradeTime(NOW.minusMinutes(50))
                .candleOpenTime(NOW.minusDays(2))
                .build();
        doReturn(Collections.singletonList(model))
                .when(redisProcessingService)
                .get(anyString(), anyString(), any(BackDealInterval.class));
        doReturn(true)
                .when(elasticsearchProcessingService)
                .exists(anyString(), anyString());
        doNothing()
                .when(elasticsearchProcessingService)
                .update(anyList(), anyString(), anyString());
        doNothing()
                .when(redisProcessingService)
                .deleteDataByHashKey(anyString(), anyString(), any(BackDealInterval.class));

        cacheDataInitializerService.cleanCache();

        verify(orderService, times(6)).getCurrencyPairsFromCache(null);
        verify(redisProcessingService, times(6)).getAllKeys(any(BackDealInterval.class));
        verify(redisProcessingService, times(3)).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, atLeastOnce()).exists(anyString(), anyString());
        verify(elasticsearchProcessingService, never()).insert(anyList(), anyString(), anyString());
        verify(redisProcessingService, atLeastOnce()).deleteDataByHashKey(anyString(), anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, atLeastOnce()).update(anyList(), anyString(), anyString());
    }

    @Test
    public void cleanCache_key_lists_empty() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .name(TEST_PAIR)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(null);
        doReturn(Collections.emptyList())
                .when(redisProcessingService)
                .getAllKeys(any(BackDealInterval.class));

        cacheDataInitializerService.cleanCache();

        verify(orderService, times(6)).getCurrencyPairsFromCache(null);
        verify(redisProcessingService, times(6)).getAllKeys(any(BackDealInterval.class));
        verify(redisProcessingService, never()).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, never()).exists(anyString(), anyString());
        verify(elasticsearchProcessingService, never()).insert(anyList(), anyString(), anyString());
        verify(elasticsearchProcessingService, never()).update(anyList(), anyString(), anyString());
        verify(redisProcessingService, never()).deleteDataByHashKey(anyString(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void cleanCache_empty_data_list() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .name(TEST_PAIR)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(null);

        key = RedisGeneratorUtil.generateKey(NOW.minusDays(10).toLocalDate());

        doReturn(Collections.singletonList(key))
                .when(redisProcessingService)
                .getAllKeys(any(BackDealInterval.class));
        doReturn(Collections.emptyList())
                .when(redisProcessingService)
                .get(anyString(), anyString(), any(BackDealInterval.class));

        cacheDataInitializerService.cleanCache();

        verify(orderService, times(6)).getCurrencyPairsFromCache(null);
        verify(redisProcessingService, times(6)).getAllKeys(any(BackDealInterval.class));
        verify(redisProcessingService, times(3)).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, never()).exists(anyString(), anyString());
        verify(elasticsearchProcessingService, never()).insert(anyList(), anyString(), anyString());
        verify(elasticsearchProcessingService, never()).update(anyList(), anyString(), anyString());
        verify(redisProcessingService, never()).deleteDataByHashKey(anyString(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void cleanDailyData_ok() {
        doReturn(Collections.singletonList(TEST_PAIR))
                .when(redisProcessingService)
                .getDailyDataKeys();
        doReturn(Collections.singletonList(DailyDataModel.builder()
                .candleOpenTime(NOW.minusDays(2))
                .highestBid(BigDecimal.TEN)
                .lowestAsk(BigDecimal.TEN)
                .build()))
                .when(redisProcessingService)
                .getDailyDataByKey(anyString());
        doNothing()
                .when(redisProcessingService)
                .deleteDailyData(anyString(), anyString());

        cacheDataInitializerService.cleanDailyData();

        verify(redisProcessingService, atLeastOnce()).getDailyDataKeys();
        verify(redisProcessingService, atLeastOnce()).getDailyDataByKey(anyString());
        verify(redisProcessingService, atLeastOnce()).deleteDailyData(anyString(), anyString());
    }
}