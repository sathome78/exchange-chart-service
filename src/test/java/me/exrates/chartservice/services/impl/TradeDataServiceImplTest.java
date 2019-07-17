package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
public class TradeDataServiceImplTest {

    private static final int CANDLES_TO_STORE = 300;
    private static final String BTC_USD = "btc_usd";
    private static final BackDealInterval MIN_INTERVAL = new BackDealInterval(5, IntervalType.MINUTE);
    private static final BackDealInterval ONE_HOUR_INTERVAL = new BackDealInterval(1, IntervalType.HOUR);

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

    }

    @Test
    public void getCandles_FromAfterTo() {
        LocalDateTime from = LocalDateTime.now();
        LocalDateTime to = from.minusMinutes(1);
        List<CandleModel> candleModel = tradeDataService.getCandles(BTC_USD, from, to, MIN_INTERVAL);
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

        doReturn(Arrays.asList(lastCandle)).when(redisProcessingService).getByRange(any(), eq(to), eq(BTC_USD), eq(ONE_HOUR_INTERVAL));
        doReturn(Arrays.asList(firstCandle, secondCandle, thirdCandle)).when(elasticsearchProcessingService).getByRange(eq(from), any(), eq(BTC_USD));

        List<CandleModel> candles = tradeDataService.getCandles(BTC_USD, from, to, ONE_HOUR_INTERVAL);

        verify(redisProcessingService, times(1)).getByRange(any(), eq(to), eq(BTC_USD), eq(ONE_HOUR_INTERVAL));
        verify(elasticsearchProcessingService, times(1)).getByRange(eq(from), any(), eq(BTC_USD));

        Assert.assertEquals(4, candles.size());
    }

    @Test
    public void handleReceivedTrades() {
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

    private BigDecimal getRandomBigDecimal() {
        return new BigDecimal(BigInteger.valueOf(new Random().nextInt(100001)), 2);
    }


    @TestConfiguration
    static class TradeDataServiceImplTestContextConfiguration {

        @MockBean
        private ElasticsearchProcessingService elasticsearchProcessingService;

        @MockBean
        private RedisProcessingService redisProcessingService;

        @MockBean
        private XSync<String> xSync;

        @MockBean
        private List<BackDealInterval> supportedIntervals;

        @Bean
        public TradeDataService employeeService() {
            return new TradeDataServiceImpl(
                    elasticsearchProcessingService,
                    redisProcessingService,
                    xSync,
                    CANDLES_TO_STORE,
                    supportedIntervals
            );
        }
    }
}
