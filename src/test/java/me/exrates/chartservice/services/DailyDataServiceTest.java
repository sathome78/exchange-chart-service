package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.CoinmarketcapApiDto;
import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.DailyDataModel;
import me.exrates.chartservice.model.ExchangeRatesDto;
import me.exrates.chartservice.services.impl.DailyDataServiceImpl;
import me.exrates.chartservice.utils.TimeUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.math.BigDecimal;
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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class DailyDataServiceTest extends AbstractTest {

    @Mock
    private RedisProcessingService redisProcessingService;
    @Mock
    private OrderService orderService;

    private DailyDataService dailyDataService;

    @Before
    public void setUp() throws Exception {
        dailyDataService = spy(new DailyDataServiceImpl(redisProcessingService, orderService));
    }

    @Test
    public void getCoinmarketcapData_all_pairs() {
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

        List<CoinmarketcapApiDto> list = dailyDataService.getCoinmarketcapData(null, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertFalse(list.isEmpty());
        assertEquals(1, list.size());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(redisProcessingService, atLeastOnce()).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, atLeastOnce()).getDailyDataByKey(anyString());
    }

    @Test
    public void getCoinmarketcapData_empty_pairs_list() {
        doReturn(Collections.emptyList())
                .when(orderService)
                .getCurrencyPairsFromCache(null);

        List<CoinmarketcapApiDto> list = dailyDataService.getCoinmarketcapData(null, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertTrue(list.isEmpty());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(redisProcessingService, never()).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, never()).getDailyDataByKey(anyString());
    }

    @Test
    public void getCoinmarketcapData_all_empty_list_from_redis() {
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

        List<CoinmarketcapApiDto> list = dailyDataService.getCoinmarketcapData(null, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertTrue(list.isEmpty());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(redisProcessingService, atLeastOnce()).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, never()).getDailyDataByKey(anyString());
    }

    @Test
    public void getCoinmarketcapData_empty_daily_data_list() {
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

        List<CoinmarketcapApiDto> list = dailyDataService.getCoinmarketcapData(null, ONE_DAY_INTERVAL);

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
    public void getCoinmarketcapData_one_pair() {
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

        List<CoinmarketcapApiDto> list = dailyDataService.getCoinmarketcapData(TEST_PAIR, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertFalse(list.isEmpty());
        assertEquals(1, list.size());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(anyString());
        verify(redisProcessingService, atLeastOnce()).get(anyString(), anyString(), any(BackDealInterval.class));
        verify(redisProcessingService, atLeastOnce()).getDailyDataByKey(anyString());
    }

    @Test
    public void getCoinmarketcapData_one_pair_empty_candle_list() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .id(1)
                .name(TEST_PAIR)
                .hidden(false)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(anyString());
        doReturn(Collections.emptyList())
                .when(redisProcessingService)
                .get(anyString(), anyString(), any(BackDealInterval.class));

        List<CoinmarketcapApiDto> list = dailyDataService.getCoinmarketcapData(TEST_PAIR, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertTrue(list.isEmpty());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(anyString());
        verify(redisProcessingService, atLeastOnce()).get(anyString(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void getExchangeRatesData_all_pairs() {
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
                .predLastRate(new BigDecimal(5000))
                .openRate(new BigDecimal(6000))
                .highRate(new BigDecimal(6000))
                .lowRate(new BigDecimal(5000))
                .lastTradeTime(NOW.plusMinutes(5))
                .firstTradeTime(NOW)
                .build()))
                .when(redisProcessingService)
                .get(anyString(), anyString(), any(BackDealInterval.class));

        List<ExchangeRatesDto> list = dailyDataService.getExchangeRatesData(null, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertFalse(list.isEmpty());
        assertEquals(1, list.size());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(redisProcessingService, atLeastOnce()).get(anyString(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void getExchangeRatesData_empty_pairs_list() {
        doReturn(Collections.emptyList())
                .when(orderService)
                .getCurrencyPairsFromCache(null);

        List<ExchangeRatesDto> list = dailyDataService.getExchangeRatesData(null, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertTrue(list.isEmpty());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(redisProcessingService, never()).get(anyString(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void getExchangeRatesData_all_empty_list_from_redis() {
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

        List<ExchangeRatesDto> list = dailyDataService.getExchangeRatesData(null, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertTrue(list.isEmpty());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(redisProcessingService, atLeastOnce()).get(anyString(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void getExchangeRatesData_one_pair() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .id(1)
                .name(TEST_PAIR)
                .scale(8)
                .matket(MARKET)
                .type(TYPE)
                .topMarket(true)
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
                .predLastRate(new BigDecimal(5000))
                .openRate(new BigDecimal(6000))
                .highRate(new BigDecimal(6000))
                .lowRate(new BigDecimal(5000))
                .lastTradeTime(NOW.plusMinutes(5))
                .firstTradeTime(NOW)
                .build()))
                .when(redisProcessingService)
                .get(anyString(), anyString(), any(BackDealInterval.class));

        List<ExchangeRatesDto> list = dailyDataService.getExchangeRatesData(BTC_USD, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertFalse(list.isEmpty());
        assertEquals(1, list.size());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(anyString());
        verify(redisProcessingService, atLeastOnce()).get(anyString(), anyString(), any(BackDealInterval.class));
    }

    @Test
    public void getExchangeRatesData_one_pair_empty_candle_list() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .id(1)
                .name(TEST_PAIR)
                .scale(8)
                .matket(MARKET)
                .type(TYPE)
                .topMarket(true)
                .hidden(false)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(anyString());
        doReturn(Collections.emptyList())
                .when(redisProcessingService)
                .get(anyString(), anyString(), any(BackDealInterval.class));

        List<ExchangeRatesDto> list = dailyDataService.getExchangeRatesData(BTC_USD, ONE_DAY_INTERVAL);

        assertNotNull(list);
        assertTrue(list.isEmpty());

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(anyString());
        verify(redisProcessingService, atLeastOnce()).get(anyString(), anyString(), any(BackDealInterval.class));
    }
}