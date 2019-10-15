package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.CoinmarketcapApiDto;
import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.DailyDataModel;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.services.impl.CoinmarketcapServiceImpl;
import me.exrates.chartservice.utils.TimeUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class CoinmarketcapServiceImplTest extends AbstractTest {

    @Mock
    private RedisProcessingService redisProcessingService;
    @Mock
    private OrderService orderService;

    private CoinmarketcapService coinmarketcapService;

    @Before
    public void setUp() throws Exception {
        coinmarketcapService = spy(new CoinmarketcapServiceImpl(redisProcessingService, orderService));
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

        coinmarketcapService.cleanDailyData();

        verify(redisProcessingService, atLeastOnce()).getDailyDataKeys();
        verify(redisProcessingService, atLeastOnce()).getDailyDataByKey(anyString());
        verify(redisProcessingService, atLeastOnce()).deleteDailyData(anyString(), anyString());
    }

    @Test
    public void getData_all_pairs() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .id(1)
                .name(TEST_PAIR)
                .hidden(false)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(null);
        doReturn(Collections.singletonList(CandleModel.builder()
                .pairName(BTC_USD)
                .volume(new BigDecimal(1))
                .currencyVolume(new BigDecimal(10))
                .candleOpenTime(TimeUtil.getNearestTimeBeforeForMinInterval(NOW))
                .closeRate(new BigDecimal(5000))
                .openRate(new BigDecimal(6000))
                .highRate(new BigDecimal(6000))
                .lowRate(new BigDecimal(5000))
                .lastTradeTime(NOW.plusMinutes(5))
                .firstTradeTime(NOW)
                .build()))
                .when(redisProcessingService)
                .get(anyString(), anyString(), any(BackDealInterval.class));
        doReturn(Collections.singletonList(DailyDataModel.builder()
                .candleOpenTime(NOW)
                .highestBid(new BigDecimal(6000))
                .lowestAsk(new BigDecimal(5000))
                .build()))
                .when(redisProcessingService)
                .getDailyDataByKey(anyString());

        List<CoinmarketcapApiDto> list = coinmarketcapService.getData(null, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertFalse(list.isEmpty());
        assertEquals(1, list.size());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(redisProcessingService, atLeastOnce()).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, atLeastOnce()).getDailyDataByKey(anyString());
    }

    @Test
    public void getData_empty_pairs_list() {
        doReturn(Collections.emptyList())
                .when(orderService)
                .getCurrencyPairsFromCache(null);

        List<CoinmarketcapApiDto> list = coinmarketcapService.getData(null, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertTrue(list.isEmpty());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(redisProcessingService, never()).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, never()).getDailyDataByKey(anyString());
    }

    @Test
    public void getData_all_empty_list_from_redis() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .id(1)
                .name(TEST_PAIR)
                .hidden(false)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(null);
        doReturn(Collections.emptyList())
                .when(redisProcessingService)
                .get(anyString(), anyString(), any(BackDealInterval.class));

        List<CoinmarketcapApiDto> list = coinmarketcapService.getData(null, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertTrue(list.isEmpty());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(redisProcessingService, atLeastOnce()).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, never()).getDailyDataByKey(anyString());
    }

    @Test
    public void getData_empty_daily_data_list() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .id(1)
                .name(TEST_PAIR)
                .hidden(false)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(null);
        doReturn(Collections.singletonList(CandleModel.builder()
                .pairName(BTC_USD)
                .volume(new BigDecimal(1))
                .currencyVolume(new BigDecimal(10))
                .candleOpenTime(TimeUtil.getNearestTimeBeforeForMinInterval(NOW))
                .closeRate(new BigDecimal(5000))
                .openRate(new BigDecimal(6000))
                .highRate(new BigDecimal(6000))
                .lowRate(new BigDecimal(5000))
                .lastTradeTime(NOW.plusMinutes(5))
                .firstTradeTime(NOW)
                .build()))
                .when(redisProcessingService)
                .get(anyString(), anyString(), any(BackDealInterval.class));
        doReturn(Collections.emptyList())
                .when(redisProcessingService)
                .getDailyDataByKey(anyString());

        List<CoinmarketcapApiDto> list = coinmarketcapService.getData(null, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertFalse(list.isEmpty());
        assertEquals(1, list.size());
        assertNull(list.get(0).getHighestBid());
        assertNull(list.get(0).getLowestAsk());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(redisProcessingService, atLeastOnce()).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, atLeastOnce()).getDailyDataByKey(anyString());
    }

    @Test
    public void getData_one_pair() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .id(1)
                .name(TEST_PAIR)
                .hidden(false)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(anyString());
        doReturn(Collections.singletonList(CandleModel.builder()
                .pairName(BTC_USD)
                .volume(new BigDecimal(1))
                .currencyVolume(new BigDecimal(10))
                .candleOpenTime(TimeUtil.getNearestTimeBeforeForMinInterval(NOW))
                .closeRate(new BigDecimal(5000))
                .openRate(new BigDecimal(6000))
                .highRate(new BigDecimal(6000))
                .lowRate(new BigDecimal(5000))
                .lastTradeTime(NOW.plusMinutes(5))
                .firstTradeTime(NOW)
                .build()))
                .when(redisProcessingService)
                .get(anyString(), anyString(), any(BackDealInterval.class));
        doReturn(Collections.singletonList(DailyDataModel.builder()
                .candleOpenTime(NOW)
                .highestBid(new BigDecimal(6000))
                .lowestAsk(new BigDecimal(5000))
                .build()))
                .when(redisProcessingService)
                .getDailyDataByKey(anyString());

        List<CoinmarketcapApiDto> list = coinmarketcapService.getData(TEST_PAIR, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertFalse(list.isEmpty());
        assertEquals(1, list.size());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(anyString());
        verify(redisProcessingService, atLeastOnce()).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, atLeastOnce()).getDailyDataByKey(anyString());
    }

    @Test
    public void generate_ok() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .id(1)
                .name(TEST_PAIR)
                .hidden(false)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(null);
        doReturn(Collections.singletonList(OrderDto.builder()
                .currencyPairName(TEST_PAIR)
                .exRate(BigDecimal.TEN)
                .amountBase(BigDecimal.ONE)
                .amountConvert(BigDecimal.TEN)
                .dateAcception(NOW.plus(10, ChronoUnit.MINUTES))
                .dateCreation(NOW.plus(10, ChronoUnit.MINUTES))
                .operationTypeId(4)
                .build()))
                .when(orderService)
                .getAllOrders(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
        doNothing()
                .when(redisProcessingService)
                .insertDailyData(any(DailyDataModel.class), anyString(), anyString());

        coinmarketcapService.generate();

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(orderService, atLeastOnce()).getAllOrders(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
        verify(redisProcessingService, atLeastOnce()).insertDailyData(any(DailyDataModel.class), anyString(), anyString());
    }

    @Test
    public void generate_empty_currencies_list() {
        doReturn(Collections.emptyList())
                .when(orderService)
                .getCurrencyPairsFromCache(null);

        coinmarketcapService.generate();

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(orderService, never()).getAllOrders(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
        verify(redisProcessingService, never()).insertDailyData(any(DailyDataModel.class), anyString(), anyString());
    }

    @Test
    public void generate_empty_orders_list() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .id(1)
                .name(TEST_PAIR)
                .hidden(false)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(null);
        doReturn(Collections.emptyList())
                .when(orderService)
                .getAllOrders(any(LocalDateTime.class), any(LocalDateTime.class), anyString());

        coinmarketcapService.generate();

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(orderService, atLeastOnce()).getAllOrders(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
        verify(redisProcessingService, never()).insertDailyData(any(DailyDataModel.class), anyString(), anyString());
    }
}