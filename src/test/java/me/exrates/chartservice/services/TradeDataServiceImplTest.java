package me.exrates.chartservice.services;

import com.antkorwin.xsync.XSync;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.OrderDataDto;
import me.exrates.chartservice.services.impl.TradeDataServiceImpl;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static me.exrates.chartservice.utils.TimeUtil.getNearestBackTimeForBackdealInterval;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TradeDataServiceImplTest extends AbstractTest {

    @Mock
    ElasticsearchProcessingService elasticsearchProcessingService;
    @Mock
    RedisProcessingService redisProcessingService;

    private TradeDataService tradeDataService;

    @Before
    public void setUp() throws Exception {
        tradeDataService = spy(new TradeDataServiceImpl(
                elasticsearchProcessingService,
                redisProcessingService,
                new XSync<>(),
                candlesToStoreInCache,
                supportedIntervals));
    }

    @Test
    public void getCandleForCurrentTime() {
        CandleModel expectedCandle = buildDefaultCandle(getNearestBackTimeForBackdealInterval(NOW, M5_INTERVAL));

        doReturn(Collections.singletonList(expectedCandle))
                .when(redisProcessingService)
                .get(anyString(), anyString(), any(BackDealInterval.class));

        CandleModel candle = tradeDataService.getCandleForCurrentTime(BTC_USD, M5_INTERVAL);

        assertEquals(expectedCandle, candle);
    }

    @Test
    public void getCandles_FromAfterTo() {
        LocalDateTime from = NOW;
        LocalDateTime to = NOW.minusMinutes(1);

        List<CandleModel> candleModel = tradeDataService.getCandles(BTC_USD, from, to, M5_INTERVAL);

        assertTrue(CollectionUtils.isEmpty(candleModel));
    }

    @Test
    public void getCandles_validDates() {
        LocalDateTime to = TimeUtil.getNearestBackTimeForBackdealInterval(NOW, ONE_HOUR_INTERVAL);
        LocalDateTime from = TimeUtil.getNearestBackTimeForBackdealInterval(to.minusHours(CANDLES_TO_STORE + 20), ONE_HOUR_INTERVAL);

        CandleModel firstModel = buildDefaultCandle(from);
        CandleModel secondModel = buildDefaultCandle(from.plusHours(1));
        CandleModel thirdModel = buildDefaultCandle(from.plusHours(3));
        CandleModel lastModel = buildDefaultCandle(to);

        doReturn(Collections.singletonList(lastModel))
                .when(redisProcessingService)
                .get(anyString(), anyString(), any(BackDealInterval.class));
        doReturn(Arrays.asList(firstModel, secondModel, thirdModel))
                .when(elasticsearchProcessingService)
                .get(anyString(), anyString());

        List<CandleModel> models = tradeDataService.getCandles(BTC_USD, from, to, ONE_HOUR_INTERVAL);

        assertFalse(models.isEmpty());

        verify(redisProcessingService, atLeast(13)).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, times(1)).get(anyString(), anyString());
    }

    @Test
    public void getLastCandleTimeBeforeDate_fromDateIsNull() {
        LocalDateTime lastCandleTimeBeforeDate = tradeDataService.getLastCandleTimeBeforeDate(BTC_USD, null, M5_INTERVAL);

        assertNull(lastCandleTimeBeforeDate);

        verify(redisProcessingService, never()).getLastCandleTimeBeforeDate(any(LocalDateTime.class), any(LocalDateTime.class), anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, never()).getLastCandleTimeBeforeDate(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
    }

    @Test
    public void getLastCandleTimeBeforeDate_dateFromRedis() {
        doReturn(NOW)
                .when(redisProcessingService)
                .getLastCandleTimeBeforeDate(any(LocalDateTime.class), any(LocalDateTime.class), anyString(), any(BackDealInterval.class));

        LocalDateTime lastCandleTimeBeforeDate = tradeDataService.getLastCandleTimeBeforeDate(BTC_USD, NOW, M5_INTERVAL);

        assertNotNull(lastCandleTimeBeforeDate);
        assertEquals(NOW, lastCandleTimeBeforeDate);

        verify(redisProcessingService, atLeastOnce()).getLastCandleTimeBeforeDate(any(LocalDateTime.class), any(LocalDateTime.class), anyString(), any(BackDealInterval.class));
        verify(elasticsearchProcessingService, never()).getLastCandleTimeBeforeDate(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
    }

    @Test
    public void getLastCandleTimeBeforeDate_dateFromElasticsearch() {
        doReturn(NOW)
                .when(elasticsearchProcessingService)
                .getLastCandleTimeBeforeDate(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
        doReturn(NOW)
                .when(redisProcessingService)
                .getFirstInitializedCandleTimeFromHistory(anyString());

        LocalDateTime lastCandleTimeBeforeDate = tradeDataService.getLastCandleTimeBeforeDate(BTC_USD, NOW.minusDays(5), M5_INTERVAL);

        assertNotNull(lastCandleTimeBeforeDate);
        assertEquals(NOW, lastCandleTimeBeforeDate);

        verify(redisProcessingService, never()).getLastCandleTimeBeforeDate(any(LocalDateTime.class), any(LocalDateTime.class), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, atLeastOnce()).getFirstInitializedCandleTimeFromHistory(anyString());
        verify(elasticsearchProcessingService, atLeastOnce()).getLastCandleTimeBeforeDate(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
    }

    @Test
    public void handleReceivedTrades() {
        LocalDateTime baseTradeTime = LocalDateTime.of(2019, 7, 18, 16, 54);
        BigDecimal group1HighRate = BigDecimal.valueOf(6500);
        BigDecimal group1LowRRate = BigDecimal.valueOf(2000);

        List<OrderDataDto> trades = new ArrayList<>();
        trades.add(buildTradeData(baseTradeTime.plusSeconds(4), BigDecimal.valueOf(1), BigDecimal.valueOf(5000), BTC_USD));
        trades.add(buildTradeData(baseTradeTime.plusSeconds(2), BigDecimal.valueOf(0.5), group1HighRate, BTC_USD));
        trades.add(buildTradeData(baseTradeTime, BigDecimal.valueOf(2), group1LowRRate, BTC_USD));

        LocalDateTime time0Candle = TimeUtil.getNearestBackTimeForBackdealInterval(baseTradeTime, M5_INTERVAL);
        LocalDateTime lastTradeTime = baseTradeTime.minusMinutes(5);
        doReturn(null)
                .when(redisProcessingService)
                .get(eq(RedisGeneratorUtil.generateKey(time0Candle.toLocalDate())), eq(RedisGeneratorUtil.generateHashKey(BTC_USD)), eq(M5_INTERVAL));

        LocalDateTime time1Candle = TimeUtil.getNearestBackTimeForBackdealInterval(baseTradeTime, M15_INTERVAL);
        CandleModel model1 = CandleModel.builder()
                .pairName(BTC_USD)
                .volume(new BigDecimal(1))
                .currencyVolume(new BigDecimal(10))
                .candleOpenTime(time1Candle)
                .closeRate(new BigDecimal(5000))
                .openRate(new BigDecimal(6000))
                .highRate(new BigDecimal(6000))
                .lowRate(new BigDecimal(5000))
                .lastTradeTime(lastTradeTime)
                .firstTradeTime(lastTradeTime.minusMinutes(5))
                .build();
        doReturn(Collections.singletonList(model1))
                .when(redisProcessingService)
                .get(eq(RedisGeneratorUtil.generateKey(time1Candle.toLocalDate())), eq(RedisGeneratorUtil.generateHashKey(BTC_USD)), eq(M15_INTERVAL));

        LocalDateTime time2Candle = TimeUtil.getNearestBackTimeForBackdealInterval(baseTradeTime, M30_INTERVAL);
        CandleModel model2 = CandleModel.builder()
                .pairName(BTC_USD)
                .volume(new BigDecimal(2))
                .currencyVolume(new BigDecimal(20))
                .candleOpenTime(time2Candle)
                .closeRate(new BigDecimal(5500))
                .openRate(new BigDecimal(5000))
                .highRate(new BigDecimal(6000))
                .lowRate(new BigDecimal(5000))
                .lastTradeTime(lastTradeTime)
                .firstTradeTime(lastTradeTime.minusMinutes(35))
                .build();
        doReturn(Collections.singletonList(model2))
                .when(redisProcessingService)
                .get(eq(RedisGeneratorUtil.generateKey(time2Candle.toLocalDate())), eq(RedisGeneratorUtil.generateHashKey(BTC_USD)), eq(M30_INTERVAL));

        LocalDateTime time3Candle = TimeUtil.getNearestBackTimeForBackdealInterval(baseTradeTime, ONE_HOUR_INTERVAL);
        CandleModel model3 = CandleModel.builder()
                .pairName(BTC_USD)
                .volume(new BigDecimal(4))
                .currencyVolume(new BigDecimal(40))
                .candleOpenTime(time3Candle)
                .closeRate(new BigDecimal(5000))
                .openRate(new BigDecimal(4500))
                .highRate(new BigDecimal(6000))
                .lowRate(new BigDecimal(4500))
                .lastTradeTime(lastTradeTime)
                .firstTradeTime(lastTradeTime.minusMinutes(65))
                .build();
        doReturn(Collections.singletonList(model3))
                .when(redisProcessingService)
                .get(eq(RedisGeneratorUtil.generateKey(time3Candle.toLocalDate())), eq(RedisGeneratorUtil.generateHashKey(BTC_USD)), eq(ONE_HOUR_INTERVAL));

        LocalDateTime time4Candle = TimeUtil.getNearestBackTimeForBackdealInterval(baseTradeTime, SIX_HOUR_INTERVAL);
        CandleModel model4 = CandleModel.builder()
                .pairName(BTC_USD)
                .volume(new BigDecimal(5))
                .currencyVolume(new BigDecimal(50))
                .candleOpenTime(time4Candle)
                .closeRate(new BigDecimal(5000))
                .openRate(new BigDecimal(4500))
                .highRate(new BigDecimal(6000))
                .lowRate(new BigDecimal(4500))
                .lastTradeTime(lastTradeTime)
                .firstTradeTime(lastTradeTime.minusMinutes(365))
                .build();
        doReturn(Collections.singletonList(model4))
                .when(redisProcessingService)
                .get(eq(RedisGeneratorUtil.generateKey(time4Candle.toLocalDate())), eq(RedisGeneratorUtil.generateHashKey(BTC_USD)), eq(SIX_HOUR_INTERVAL));

        LocalDateTime time5Candle = TimeUtil.getNearestBackTimeForBackdealInterval(baseTradeTime, ONE_DAY_INTERVAL);
        CandleModel model5 = CandleModel.builder()
                .pairName(BTC_USD)
                .volume(new BigDecimal(8))
                .currencyVolume(new BigDecimal(80))
                .candleOpenTime(time5Candle)
                .closeRate(new BigDecimal(5400))
                .openRate(new BigDecimal(5000))
                .highRate(new BigDecimal(8000))
                .lowRate(new BigDecimal(1100))
                .lastTradeTime(lastTradeTime)
                .firstTradeTime(lastTradeTime.minusMinutes(725))
                .build();
        doReturn(Collections.singletonList(model5))
                .when(redisProcessingService)
                .get(eq(RedisGeneratorUtil.generateKey(time5Candle.toLocalDate())), eq(RedisGeneratorUtil.generateHashKey(BTC_USD)), eq(ONE_DAY_INTERVAL));

        tradeDataService.handleReceivedTrades(BTC_USD, trades);

        verify(redisProcessingService, times(supportedIntervals.size())).insertOrUpdate(anyList(), anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, times(supportedIntervals.size())).get(anyString(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void defineAndSaveLastInitializedCandleTime() {
        List<CandleModel> models = new ArrayList<>();
        LocalDateTime time = TimeUtil.getNearestTimeBeforeForMinInterval(NOW);

        models.add(buildDefaultCandle(time));
        models.add(buildDefaultCandle(time.plusMinutes(1)));
        models.add(buildDefaultCandle(time.plusMinutes(3)));

        doReturn(null)
                .when(redisProcessingService)
                .getLastInitializedCandleTimeFromCache(anyString());
        doNothing()
                .when(redisProcessingService)
                .insertLastInitializedCandleTimeToCache(anyString(), any(LocalDateTime.class));

        tradeDataService.defineAndSaveLastInitializedCandleTime(RedisGeneratorUtil.generateHashKey(BTC_USD), models);

        verify(redisProcessingService, times(1)).getLastInitializedCandleTimeFromCache(anyString());
        verify(redisProcessingService, times(1)).insertLastInitializedCandleTimeToCache(anyString(), any(LocalDateTime.class));
    }

    @Test
    public void defineAndSaveLastInitializedCandleTime_empty_candles() {
        tradeDataService.defineAndSaveLastInitializedCandleTime(RedisGeneratorUtil.generateHashKey(BTC_USD), Collections.emptyList());

        verify(redisProcessingService, never()).getLastInitializedCandleTimeFromCache(anyString());
        verify(redisProcessingService, never()).insertLastInitializedCandleTimeToCache(anyString(), any(LocalDateTime.class));
    }

    @Test
    public void defineAndSaveFirstInitializedCandleTime() {
        List<CandleModel> models = new ArrayList<>();
        LocalDateTime time = TimeUtil.getNearestTimeBeforeForMinInterval(NOW);

        models.add(buildDefaultCandle(time));
        models.add(buildDefaultCandle(time.plusMinutes(1)));
        models.add(buildDefaultCandle(time.plusMinutes(3)));

        doReturn(null)
                .when(redisProcessingService)
                .getFirstInitializedCandleTimeFromHistory(anyString());
        doNothing()
                .when(redisProcessingService)
                .insertFirstInitializedCandleTimeToHistory(anyString(), any(LocalDateTime.class));

        tradeDataService.defineAndSaveFirstInitializedCandleTime(RedisGeneratorUtil.generateHashKey(BTC_USD), models);

        verify(redisProcessingService, times(1)).getFirstInitializedCandleTimeFromHistory(anyString());
        verify(redisProcessingService, times(1)).insertFirstInitializedCandleTimeToHistory(anyString(), any(LocalDateTime.class));
    }

    @Test
    public void defineAndSaveFirstInitializedCandleTime_empty_candles() {
        tradeDataService.defineAndSaveFirstInitializedCandleTime(RedisGeneratorUtil.generateHashKey(BTC_USD), Collections.emptyList());

        verify(redisProcessingService, never()).getFirstInitializedCandleTimeFromHistory(anyString());
        verify(redisProcessingService, never()).insertFirstInitializedCandleTimeToHistory(anyString(), any(LocalDateTime.class));
    }

    private CandleModel buildDefaultCandle(LocalDateTime candleTime) {
        return buildDefaultCandle(candleTime, candleTime, candleTime);
    }

    private CandleModel buildDefaultCandle(LocalDateTime candleTime, LocalDateTime firstTradeTime, LocalDateTime lastTradeTime) {
        BigDecimal highRate = getRandomBigDecimal();
        return CandleModel.builder()
                .pairName(BTC_USD)
                .firstTradeTime(firstTradeTime)
                .lastTradeTime(lastTradeTime)
                .candleOpenTime(candleTime)
                .volume(getRandomBigDecimal())
                .currencyVolume(getRandomBigDecimal())
                .highRate(highRate)
                .lowRate(highRate.divide(BigDecimal.valueOf(2)))
                .closeRate(getRandomBigDecimal())
                .openRate(getRandomBigDecimal())
                .build();
    }

    private OrderDataDto buildTradeData(LocalDateTime tradeTime, BigDecimal amount, BigDecimal exrate, String pairName) {
        return OrderDataDto.builder()
                .tradeDate(tradeTime)
                .amountBase(amount)
                .amountConvert(amount)
                .exrate(exrate)
                .currencyPairName(pairName)
                .build();
    }

    private BigDecimal getRandomBigDecimal() {
        return new BigDecimal(BigInteger.valueOf(new Random().nextInt(100001)), 2);
    }
}