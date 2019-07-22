package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
public class TradeDataServiceImplTest {

    private static final int CANDLES_TO_STORE = 300;
    private static final String BTC_USD = "btc_usd";
    private static final BackDealInterval M5_INTERVAL = new BackDealInterval(5, IntervalType.MINUTE);
    private static final BackDealInterval M15_INTERVAL = new BackDealInterval(15, IntervalType.MINUTE);
    private static final BackDealInterval M30_INTERVAL = new BackDealInterval(30, IntervalType.MINUTE);
    private static final BackDealInterval ONE_HOUR_INTERVAL = new BackDealInterval(1, IntervalType.HOUR);
    private static final BackDealInterval SIX_HOUR_INTERVAL = new BackDealInterval(6, IntervalType.HOUR);
    private static final BackDealInterval ONE_DAY_INTERVAL = new BackDealInterval(1, IntervalType.DAY);

    @Autowired
    private TradeDataService tradeDataService;
    @Autowired
    private ElasticsearchProcessingService elasticsearchProcessingService;
    @Autowired
    private RedisProcessingService redisProcessingService;
    @Autowired
    private XSync<String> xSync;
    @Autowired
    private List<BackDealInterval> supportedIntervals;


    @Test
    public void getCandleForCurrentTime() {
        CandleModel expectedCandle = buildDefaultCandle(LocalDateTime.now());

        doReturn(expectedCandle).when(redisProcessingService).get(eq(BTC_USD), any(), eq(M5_INTERVAL));

        CandleModel candle = tradeDataService.getCandleForCurrentTime(BTC_USD, M5_INTERVAL);

        Assert.assertEquals(expectedCandle, candle);
    }

    @Test
    public void getCandles_FromAfterTo() {
        LocalDateTime from = LocalDateTime.now();
        LocalDateTime to = from.minusMinutes(1);
        List<CandleModel> candleModel = tradeDataService.getCandles(BTC_USD, from, to, M5_INTERVAL);
        Assert.assertNull(candleModel);
    }

    @Test
    public void getCandles_validDates() {
        LocalDateTime to = TimeUtil.getNearestBackTimeForBackdealInterval(LocalDateTime.now(), ONE_HOUR_INTERVAL);
        LocalDateTime from = TimeUtil.getNearestBackTimeForBackdealInterval(to.minusHours(CANDLES_TO_STORE + 20), ONE_HOUR_INTERVAL);

        CandleModel firstCandle = buildDefaultCandle(from);
        CandleModel secondCandle = buildDefaultCandle(from.plusHours(1));
        CandleModel thirdCandle = buildDefaultCandle(from.plusHours(3));
        CandleModel lastCandle = buildDefaultCandle(to);

        doReturn(Collections.singletonList(lastCandle)).when(redisProcessingService).getByRange(any(), eq(to), eq(BTC_USD), eq(ONE_HOUR_INTERVAL));
        doReturn(Arrays.asList(firstCandle, secondCandle, thirdCandle)).when(elasticsearchProcessingService).getByRange(eq(from), any(), eq(BTC_USD));

        List<CandleModel> candles = tradeDataService.getCandles(BTC_USD, from, to, ONE_HOUR_INTERVAL);

        verify(redisProcessingService, times(1)).getByRange(any(), eq(to), eq(BTC_USD), eq(ONE_HOUR_INTERVAL));
        verify(elasticsearchProcessingService, times(1)).getByRange(eq(from), any(), eq(BTC_USD));

        Assert.assertEquals(4, candles.size());
    }

    @Test
    public void handleReceivedTrades() {
        LocalDateTime baseTradeTime = LocalDateTime.of(2019, 7, 18, 16, 54);
        BigDecimal group1HighRate = BigDecimal.valueOf(6500);
        BigDecimal group1LowRRate = BigDecimal.valueOf(2000);

        List<TradeDataDto> trades = new ArrayList<>();
        trades.add(buildTradeData(baseTradeTime.plusSeconds(4), BigDecimal.valueOf(1), BigDecimal.valueOf(5000), BTC_USD));
        trades.add(buildTradeData(baseTradeTime.plusSeconds(2), BigDecimal.valueOf(0.5), group1HighRate, BTC_USD));
        trades.add(buildTradeData(baseTradeTime, BigDecimal.valueOf(2),group1LowRRate, BTC_USD ));

        LocalDateTime time0Candle = TimeUtil.getNearestBackTimeForBackdealInterval(baseTradeTime, M5_INTERVAL);
        LocalDateTime lastTradeTime = baseTradeTime.minusMinutes(5);
        doReturn(null)
                .when(redisProcessingService).get(eq(BTC_USD), eq(RedisGeneratorUtil.generateHashKey(time0Candle)), eq(M5_INTERVAL));

        LocalDateTime time1Candle = TimeUtil.getNearestBackTimeForBackdealInterval(baseTradeTime, M15_INTERVAL);
        doReturn(CandleModel.builder()
                            .volume(new BigDecimal(1))
                            .candleOpenTime(time1Candle)
                            .closeRate(new BigDecimal(5000))
                            .openRate(new BigDecimal(6000))
                            .highRate(new BigDecimal(6000))
                            .lowRate(new BigDecimal(5000))
                            .lastTradeTime(lastTradeTime)
                            .firstTradeTime(lastTradeTime.minusMinutes(5))
                            .build())
                .when(redisProcessingService).get(eq(BTC_USD), eq(RedisGeneratorUtil.generateHashKey(time1Candle)), eq(M15_INTERVAL));

        LocalDateTime time2Candle = TimeUtil.getNearestBackTimeForBackdealInterval(baseTradeTime, M30_INTERVAL);
        doReturn(CandleModel.builder()
                            .volume(new BigDecimal(2))
                            .candleOpenTime(time2Candle)
                            .closeRate(new BigDecimal(5500))
                            .openRate(new BigDecimal(5000))
                            .highRate(new BigDecimal(6000))
                            .lowRate(new BigDecimal(5000))
                            .lastTradeTime(lastTradeTime)
                            .firstTradeTime(lastTradeTime.minusMinutes(35))
                            .build())
                .when(redisProcessingService).get(eq(BTC_USD), eq(RedisGeneratorUtil.generateHashKey(time2Candle)), eq(M30_INTERVAL));

        LocalDateTime time3Candle = TimeUtil.getNearestBackTimeForBackdealInterval(baseTradeTime, ONE_HOUR_INTERVAL);
        doReturn(CandleModel.builder()
                            .volume(new BigDecimal(4))
                            .candleOpenTime(time3Candle)
                            .closeRate(new BigDecimal(5000))
                            .openRate(new BigDecimal(4500))
                            .highRate(new BigDecimal(6000))
                            .lowRate(new BigDecimal(4500))
                            .lastTradeTime(lastTradeTime)
                            .firstTradeTime(lastTradeTime.minusMinutes(65))
                            .build())
                .when(redisProcessingService).get(eq(BTC_USD), eq(RedisGeneratorUtil.generateHashKey(time3Candle)), eq(ONE_HOUR_INTERVAL));

        LocalDateTime time4Candle = TimeUtil.getNearestBackTimeForBackdealInterval(baseTradeTime, SIX_HOUR_INTERVAL);
        doReturn(CandleModel.builder()
                            .volume(new BigDecimal(5))
                            .candleOpenTime(time4Candle)
                            .closeRate(new BigDecimal(5000))
                            .openRate(new BigDecimal(4500))
                            .highRate(new BigDecimal(6000))
                            .lowRate(new BigDecimal(4500))
                            .lastTradeTime(lastTradeTime)
                            .firstTradeTime(lastTradeTime.minusMinutes(365))
                            .build())
                .when(redisProcessingService).get(eq(BTC_USD), eq(RedisGeneratorUtil.generateHashKey(time4Candle)), eq(SIX_HOUR_INTERVAL));

        LocalDateTime time5Candle = TimeUtil.getNearestBackTimeForBackdealInterval(baseTradeTime, ONE_DAY_INTERVAL);
        doReturn(CandleModel.builder()
                            .volume(new BigDecimal(8))
                            .candleOpenTime(time5Candle)
                            .closeRate(new BigDecimal(5400))
                            .openRate(new BigDecimal(5000))
                            .highRate(new BigDecimal(8000))
                            .lowRate(new BigDecimal(1100))
                            .lastTradeTime(lastTradeTime)
                            .firstTradeTime(lastTradeTime.minusMinutes(725))
                            .build())
                .when(redisProcessingService).get(eq(BTC_USD), eq(RedisGeneratorUtil.generateHashKey(time5Candle)), eq(ONE_DAY_INTERVAL));

        CandleModel expectedCandle5 = CandleModel.builder()
                .volume(new BigDecimal(3.5))
                .candleOpenTime(time0Candle)
                .closeRate(new BigDecimal(5000)) //+
                .openRate(group1LowRRate)
                .highRate(group1HighRate)
                .lowRate(group1LowRRate)
                .lastTradeTime(baseTradeTime.plusSeconds(4)) //+
                .firstTradeTime(baseTradeTime)
                .build();

        CandleModel expectedCandle15 = CandleModel.builder()
                .volume(new BigDecimal(4.5))
                .candleOpenTime(time1Candle)
                .closeRate(new BigDecimal(5000))
                .openRate(new BigDecimal(6000))
                .highRate(group1HighRate)
                .lowRate(group1LowRRate)
                .lastTradeTime(baseTradeTime.plusSeconds(4))
                .firstTradeTime(lastTradeTime.minusMinutes(5))
                .build();

        CandleModel expectedCandle30 = CandleModel.builder()
                .volume(new BigDecimal(5.5))
                .candleOpenTime(time2Candle)
                .closeRate(new BigDecimal(5000))
                .openRate(new BigDecimal(5000))
                .highRate(group1HighRate)
                .lowRate(group1LowRRate)
                .lastTradeTime(baseTradeTime.plusSeconds(4))
                .firstTradeTime(lastTradeTime.minusMinutes(35))
                .build();

        CandleModel expectedCandle1h = CandleModel.builder()
                .volume(new BigDecimal(7.5))
                .candleOpenTime(time3Candle)
                .closeRate(new BigDecimal(5000))
                .openRate(new BigDecimal(4500))
                .highRate(group1HighRate)
                .lowRate(group1LowRRate)
                .lastTradeTime(baseTradeTime.plusSeconds(4))
                .firstTradeTime(lastTradeTime.minusMinutes(65))
                .build();

        CandleModel expectedCandle6h = CandleModel.builder()
                .volume(new BigDecimal(8.5))
                .candleOpenTime(time4Candle)
                .closeRate(new BigDecimal(5000))
                .openRate(new BigDecimal(4500))
                .highRate(group1HighRate)
                .lowRate(group1LowRRate)
                .lastTradeTime(baseTradeTime.plusSeconds(4))
                .firstTradeTime(lastTradeTime.minusMinutes(365))
                .build();

        CandleModel expectedCandle1D = CandleModel.builder()
                .volume(new BigDecimal(11.5))
                .candleOpenTime(time5Candle)
                .closeRate(new BigDecimal(5000))
                .openRate(new BigDecimal(5000))
                .highRate(new BigDecimal(8000))
                .lowRate(new BigDecimal(1100))
                .lastTradeTime(baseTradeTime.plusSeconds(4))
                .firstTradeTime(lastTradeTime.minusMinutes(725))
                .build();


        tradeDataService.handleReceivedTrades(BTC_USD, trades);

        verify(redisProcessingService, times(supportedIntervals.size())).insertOrUpdate(any(), any(), any());
        verify(redisProcessingService, times(supportedIntervals.size())).get(any(), any(), any());

        verify(redisProcessingService, times(1)).insertOrUpdate(expectedCandle5, BTC_USD, M5_INTERVAL);
        verify(redisProcessingService, times(1)).insertOrUpdate(expectedCandle15, BTC_USD,  M15_INTERVAL);
        verify(redisProcessingService, times(1)).insertOrUpdate(expectedCandle30, BTC_USD,  M30_INTERVAL);
        verify(redisProcessingService, times(1)).insertOrUpdate(expectedCandle1h, BTC_USD,  ONE_HOUR_INTERVAL);
        verify(redisProcessingService, times(1)).insertOrUpdate(expectedCandle6h, BTC_USD,  SIX_HOUR_INTERVAL);
        verify(redisProcessingService, times(1)).insertOrUpdate(expectedCandle1D, BTC_USD,  ONE_DAY_INTERVAL);
    }

    @Test
    public void defineAndSaveLastInitializedCandleTime() {
        List<CandleModel> candleModels = new ArrayList<>();
        LocalDateTime time = TimeUtil.getNearestTimeBeforeForMinInterval(LocalDateTime.now());

        candleModels.add(buildDefaultCandle(time));
        candleModels.add(buildDefaultCandle(time.plusMinutes(1)));
        candleModels.add(buildDefaultCandle(time.plusMinutes(3)));

        tradeDataService.defineAndSaveLastInitializedCandleTime(BTC_USD, candleModels);
        verify(redisProcessingService, times(1))
                .insertLastInitializedCandleTimeToCache(BTC_USD, time.plusMinutes(3));
    }

    @Test
    public void defineAndSaveLastInitializedCandleTime_EMPTY_CNADLES() {
        tradeDataService.defineAndSaveLastInitializedCandleTime(BTC_USD, Collections.emptyList());
        verify(redisProcessingService, never())
                .insertLastInitializedCandleTimeToCache(eq(BTC_USD), any());
    }

    private CandleModel buildDefaultCandle(LocalDateTime candleTime) {
        return buildDefaultCandle(candleTime, candleTime, candleTime);
    }

    private CandleModel buildDefaultCandle(LocalDateTime candleTime, LocalDateTime firstTradeTime, LocalDateTime lastTradeTime) {
        BigDecimal highRate = getRandomBigDecimal();
        return CandleModel.builder()
                .firstTradeTime(firstTradeTime)
                .lastTradeTime(lastTradeTime)
                .candleOpenTime(candleTime)
                .volume(getRandomBigDecimal())
                .highRate(highRate)
                .lowRate(highRate.divide(BigDecimal.valueOf(2)))
                .closeRate(getRandomBigDecimal())
                .openRate(getRandomBigDecimal())
                .build();
    }

    private TradeDataDto buildTradeData(LocalDateTime tradeTime, BigDecimal amount, BigDecimal exrate, String pairName) {
        return new TradeDataDto(pairName, exrate, amount, tradeTime);
    }

    private BigDecimal getRandomBigDecimal() {
        return new BigDecimal(BigInteger.valueOf(new Random().nextInt(100001)), 2);
    }


    @TestConfiguration
    static class TradeDataServiceImplTestContextConfiguration {

        @MockBean
        private ElasticsearchProcessingService elasticsearchProcessingService;

        @MockBean
        private RedisProcessingService redisProcessingService;

        @Bean
        public XSync<String> xSync() {
            return new XSync<>();
        }

        @Bean
        public List<BackDealInterval> supportedIntervals() {
            return Stream.of(IntervalType.values())
                    .map(p -> (IntStream.of(p.getSupportedValues())
                            .mapToObj(v -> new BackDealInterval(v, p))
                            .collect(Collectors.toList())))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }

        @Bean
        public TradeDataService employeeService() {
            return new TradeDataServiceImpl(
                    elasticsearchProcessingService,
                    redisProcessingService,
                    xSync(),
                    CANDLES_TO_STORE,
                    supportedIntervals()
            );
        }
    }
}
